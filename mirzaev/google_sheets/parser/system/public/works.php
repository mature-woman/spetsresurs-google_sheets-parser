<?php

// Фреймворк ArangoDB
use mirzaev\arangodb\connection,
	mirzaev\arangodb\collection,
	mirzaev\arangodb\document;

// Библиотека для ArangoDB
use ArangoDBClient\Document as _document;

// Фреймворк для Google Sheets
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetRange,
	Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor,
	Flow\ETL\Adapter\GoogleSheet\Columns,
	Flow\ETL\Flow,
	Flow\ETL\Config,
	Flow\ETL\FlowContext,
	Flow\ETL\Row\Entry,
	Flow\ETL\Row,
	Flow\ETL\DSL\To,
	Flow\ETL\DSL\From;

// Фреймворк для Google API
use Google\Client,
	Google\Service\Sheets,
	Google\Service\Sheets\ValueRange;

require __DIR__ . '/../../../../../vendor/autoload.php';

$arangodb = new connection(require '../settings/arangodb.php');

function generateLabel(string $name): string
{
	return match ($name) {
		'_id', 'ID'  => '_id',
		'market', 'Магазин' => 'market',
		'worker', 'Сотрудник' => 'worker',
		'work', 'Работа' => 'work',
		'date', 'Дата' => 'date',
		'start', 'Начало' => 'start',
		'end', 'Конец' => 'end',
		'confirmed', 'Подтверждено' => 'confirmed',
		'commentary', 'Комментарий' => 'commentary',
		'response', 'Ответ' => 'response',
		default => throw new exception("Неизвестный столбец: $name")
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'ID', '_id' => 'ID',
		'Магазин', 'market' => 'Магазин',
		'Сотрудник', 'worker' => 'Сотрудник',
		'Работа', 'work' => 'Работа',
		'Дата', 'date' => 'Дата',
		'Начало', 'start' => 'Начало',
		'Конец', 'end' => 'Конец',
		'Подтверждено', 'confirmed' => 'Подтверждено',
		'Комментарий', 'commentary' => 'Комментарий',
		'Ответ', 'response' => 'Ответ',
		default => throw new exception("Неизвестный столбец: $name")
	};
}

function init(array $row, bool $reverse = false): array
{
	$buffer = [];

	foreach ($row as $key => $value) $buffer[(($reverse ? 'de' : null) . 'generateLabel')($key)] = $value;

	return $buffer;
}


function sync(Row &$row): void
{
	global $arangodb;

	$_row = init($row->entries()->toArray()['row']);

	if (collection::init($arangodb->session, 'works'))
		if (!empty($_row['_id']) && $work = collection::search($arangodb->session, sprintf("FOR d IN works FILTER d._id == '%s' RETURN d", $_row['_id']))) {
			// Найдена запись работы (строки) в базе данных

			// Очистка перед записью в таблицу
			$new = array_diff_key($work->getAll(), ['_key' => true, 'created' => true]);

			// Инициализация выбранного сотрудника
			if (collection::init($arangodb->session, 'readinesses', true)	&& collection::init($arangodb->session, 'workers'))
				$new = ['worker' => collection::search(
					$arangodb->session,
					sprintf(
						"FOR d IN workers LET e = (FOR e IN readinesses FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
						$_row['_id']
					)
				)->id ?? ''] + $new;
			else throw new exception('Не удалось инициализировать коллекции');

			// Инициализация магазина
			if (collection::init($arangodb->session, 'requests', true)	&& collection::init($arangodb->session, 'markets'))
				if ($market = collection::search(
					$arangodb->session,
					sprintf(
						"FOR d IN markets LET e = (FOR e IN requests FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
						$_row['_id']
					)
				)) $new = ['market' => $market->id] + $new;
				else throw new exception('Не удалось найти магазин');
			else throw new exception('Не удалось инициализировать коллекции');

			// Запись идентификатора только что созданной записи в базе данных для записи в таблицу
			$new = ['_id' => $work->getId()] + $new;

			// Реинициализация строки с новыми данными по ссылке (приоритет из базы данных)
			if ($_row !== $new) $row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));
		} else	if (
			!empty($_row['market'])
			&& collection::init($arangodb->session, 'requests', true)	&& collection::init($arangodb->session, 'markets')
			&& ($market = collection::search($arangodb->session,	sprintf("FOR d IN markets FILTER d.id == '%s' RETURN d", $_row['market'])))
			&& $work = collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN works FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session,	'works', array_diff_key($_row, ['_id' => true, 'market' => true, 'worker' => true]))
				)
			)
		) {
			// Не найдена запись работы (строки) в базе данных и была создана

			// Инициализация ребра: market -> work (запрос магазина о работе)
			if (collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN requests FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session, 'requests', ['_from' => $market->getId(), '_to' => $work->getId()])
				)
			));
			else throw new exception('Не удалось создать заявку магазина');

			if (
				!empty($_row['worker'])
				&& collection::init($arangodb->session, 'readinesses', true)	&& collection::init($arangodb->session, 'workers')
				&& ($worker = collection::search($arangodb->session,	sprintf("FOR d IN workers FILTER d.id == '%s' RETURN d", $_row['worker'])))
			) {
				// Инициализация ребра: workers -> work (готовность работника приступать к заявке)
				if (collection::search(
					$arangodb->session,
					sprintf(
						"FOR d IN readinesses FILTER d._id == '%s' RETURN d",
						document::write($arangodb->session, 'readinesses', ['_from' => $worker->getId(), '_to' => $work->getId()])
					)
				));
				else throw new exception('Не удалось создать готовность сотрудника');
			}

			// Реинициализация строки с новыми данными по ссылке (приоритет из базы данных)
			$row = $row->set((new Flow())->read(From::array([init(['_id' => $work->getId()] + $_row, true)]))->fetch(1)[0]->get('row'));
		} else return;
	else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require('../settings/works/google.php'), true);
$document = require('../settings/works/document.php');
$sheets = require('../settings/works/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);

foreach ($sheets as $sheet) {
	$sheets = new Sheets($client);

	$rows = (new Flow())->read(new GoogleSheetExtractor($sheets, $document, new Columns($sheet, 'A', 'J'), true, 1000, 'row'));

	$i = 1;

	foreach ($rows->fetch(10000) as $row) {
		++$i;
		$buffer = $row;
		sync($row);
		if ($buffer !== $row)
			$sheets->spreadsheets_values->update(
				$document,
				"$sheet!A$i:J$i",
				new ValueRange(['values' => [array_values($row->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);
	}
}
