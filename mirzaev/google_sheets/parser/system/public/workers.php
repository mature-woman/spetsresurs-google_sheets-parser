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
		'id', 'ID', '1', '11', '111', '1111', '11111', 'index', 'Index', 'код', 'Код', 'ключ', 'Ключ', 'айди', 'Айди', 'идентификатор', 'Идентификатор' => 'id',
		'name', 'ФИО', 'фио', 'ф.и.о.', 'ф. и. о.', 'Ф.И.О.', 'Ф. И. О.' => 'name',
		'phone', 'Номер', 'номер', 'телефон', 'Телефон' => 'phone',
		'birth', 'Дата рождения', 'дата рождения', 'Год рождения', 'год рождения', 'Год', 'год', 'День рождения', 'день рождения' => 'birth',
		'address', 'Адрес регистрации', 'адрес регистрации', 'Адрес', 'адрес', 'Регистрация', 'регистрация' => 'address',
		'commentary', 'Комментарий', 'ПРИМЕЧАНИЕ', 'Примечание', 'примечание' => 'commentary',
		'activity', 'Работа', 'работа', 'Вид Работы', 'Вид работы', 'вид работы' => 'activity',
		'passport', 'Паспорт', 'паспорт', 'серия и номер паспорта', 'Серия и номер паспорта' => 'passport',
		'issued', 'Выдан', 'выдан' => 'issued',
		'department', 'Код подразделения', 'код подразделения', 'Подразделение', 'подразделение' => 'department',
		'hiring', 'Дата присоединения', 'Когда устроили', 'когда устроили' => 'hiring',
		'district', 'Район', 'район' => 'district',
		'requisites', 'Реквизиты', 'реквизиты' => 'requisites',
		'fired', 'Дата увольнения', 'дата увольнения', 'Уволен', 'уволен', 'Увольнение', 'увольнение' => 'fired',
		'payment', 'Оплата', 'оплата' => 'payment',
		'tax', 'ИНН', 'инн' => 'tax',
		default => throw new exception("Неизвестный столбец: $name")
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'ID', 'id' => 'ID',
		'ФИО', 'name' => 'ФИО',
		'Номер', 'phone' => 'Номер',
		'Дата рождения', 'birth' => 'Дата рождения',
		'Адрес регистрации', 'address' => 'Адрес регистрации',
		'Комментарий', 'commentary' => 'Комментарий',
		'Работа', 'activity' => 'Работа',
		'Паспорт', 'passport' => 'Паспорт',
		'Выдан', 'issued' => 'Выдан',
		'Код подразделения', 'department' => 'Код подразделения',
		'Дата присоединения', 'hiring' => 'Дата присоединения',
		'Район', 'district' => 'Район',
		'Реквизиты', 'requisites' => 'Реквизиты',
		'Дата увольнения', 'fired' => 'Дата увольнения',
		'Оплата', 'payment' => 'Оплата',
		'ИНН', 'tax' => 'ИНН',
		default => throw new exception("Неизвестный столбец: $name")
	};
}

function init(array $row, bool $reverse = false): array
{
	$buffer = [];

	foreach ($row as $key => $value) $buffer[(($reverse ? 'de' : null) . 'generateLabel')($key)] = $value ?? '';

	return $buffer;
}

function connect(_document $worker, _document $robot): void
{
	global $arangodb;

	if (
		collection::init($arangodb->session, 'connections', true)
		&& (collection::search(
			$arangodb->session,
			sprintf(
				"FOR d IN connections FILTER d._from == '%s' && d._to == '%s' RETURN d",
				$worker->getId(),
				$robot->getId()
			)
		)
			?? collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN connections FILTER d._id == '%s' RETURN d",
					document::write(
						$arangodb->session,
						'connections',
						['_from' => $worker->getId(), '_to' => $robot->getId()]
					)
				)
			))
	) {
		// Инициализировано ребро: workers -> robot (любой)

		// Активация
		$robot->status = 'active';
		document::update($arangodb->session, $robot);
	}
}

function connectAll(_document $worker): void
{
	global $arangodb;

	// Инициализация ребра: workers -> viber
	if (
		collection::init($arangodb->session, 'viber')
		&& $viber = collection::search(
			$arangodb->session,
			sprintf(
				"FOR d IN viber FILTER d.number == '%d' RETURN d",
				$worker->phone
			)
		)
	) connect($worker, $viber);
}


function sync(Row &$row, string $city = 'Красноярск'): void
{
	global $arangodb;

	$_row = init($row->entries()->toArray()['row']);

	if ($_row['id'] !== null)
		if (collection::init($arangodb->session, 'workers'))
			if ($worker = collection::search($arangodb->session, sprintf("FOR d IN workers FILTER d.id == '%s' RETURN d", $_row['id']))
				?? collection::search($arangodb->session, sprintf("FOR d IN workers FILTER d._id == '%s' RETURN d", document::write($arangodb->session, 'workers', $_row + ['city' => $city])))
			) {
				// Инициализирован работник

				// Реинициализация строки с актуальными записями (приоритет у базы данных)
				if ($_row !== $new = array_diff_key($worker->getAll(), ['_key' => true, 'created' => true, 'city' => true]))
					$row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));

				// Подключение к чат-роботам
				connectAll($worker);
			} else throw new exception('Не удалось создать или найти созданного работника');
		else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require('../settings/workers/google.php'), true);
$document = require('../settings/workers/document.php');
$sheets = require('../settings/workers/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);
$api = new Sheets($client);

foreach ($sheets as $sheet) {
	$rows = (new Flow())->read(new GoogleSheetExtractor($api, $document, new Columns($sheet, 'A', 'P'), true, 1000, 'row'));

	$i = 1;
	foreach ($rows->fetch(5000) as $row) {
		++$i;
		$buffer = $row;
		sync($row, $sheet);
		if ($buffer !== $row) {
			$api->spreadsheets_values->update(
				$document,
				"$sheet!A$i:P$i",
				new ValueRange(['values' => [array_values($row->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);
		}
	}
}
