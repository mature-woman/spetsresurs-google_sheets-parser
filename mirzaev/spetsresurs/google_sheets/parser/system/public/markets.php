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

require __DIR__ . '/../../../../../../vendor/autoload.php';

$arangodb = new connection(require __DIR__ . '/../settings/arangodb.php');

function generateLabel(string $name): string
{
	return match ($name) {
		'id', 'ID', 'ТТ'  => 'id',
		'type', 'ТИП', 'Тип', 'тип' => 'type',
		'name', 'ДИРЕКТОР', 'Директор', 'директор' => 'name',
		'address', 'АДРЕС', 'Адрес', 'адрес' => 'address',
		default => $name
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'ID', 'id' => 'ID',
		'ТИП', 'type' => 'ТИП',
		'ДИРЕКТОР', 'name' => 'ДИРЕКТОР',
		'АДРЕС', 'address' => 'АДРЕС',
		default => $name
	};
}

function convertNumber(string $number): string
{
	// Очистка всего кроме цифр, а потом поиск 10 первых чисел (без восьмёрки)
	preg_match('/\d?(\d{10})/', preg_replace("/[^\d]/", "", $number), $matches);

	// Инициализация номера
	$number = isset($matches[1]) ? 7 . $matches[1] : $number;

	return $number;
}

function init(array $row, bool $reverse = false): array
{
	$buffer = [];

	foreach ($row as $key => $value) $buffer[(($reverse ? 'de' : null) . 'generateLabel')($key)] = $value ?? '';

	return $buffer;
}


function sync(Row &$row, string $city = 'Красноярск'): void
{
	global $arangodb;

	// Инициализация строки в Google Sheet
	$_row = init($row->toArray()['row']);

	// Инициализация ФИО
	$name = explode(' ', $_row['name']);

	if (collection::init($arangodb->session, 'markets'))
		if (!empty($_row['id']) && $market = collection::search($arangodb->session, sprintf("FOR d IN markets FILTER d.id == '%s' RETURN d", $_row['id']))) {
			// Найдена запись магазина (строки) в базе данных и включен режим перезаписи (приоритет - google sheets)

			if (false && $market->transfer_to_sheets) {
				// Запрошен форсированный перенос данных из базы данных в таблицу

				/* // Инициализация данных для записи в таблицу
				$new = [
					'id' => $market->id ?? '',
					'type' => $market->type ?? '',
					'director' => $market->director ?? '',
					'address' => $market->address ?? '',
				];

				// Замена NULL на пустую строку
				foreach ($new as $key => &$value) if ($value === null) $value = '';

				// Реинициализация строки с новыми данными по ссылке (приоритет из базы данных)
				if ($_row !== $new) $row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));

				// Деактивация форсированного трансфера
				$market->transfer_to_sheets = false; */
			} else {
				// Перенос изменений из Google Sheet в инстанцию документа в базе данных

				/* // Реинициализация данных в инстанции документа в базе данных с данными из Google Sheet
				foreach ($market->getAll() as $key => $value) {
					// Перебор всех записанных значений в инстанции документа в базе данных

					// Конвертация
					$market->{$key} = $_row[$key] ?? $value;
				} */
			}

			/* // Обновление инстанции документа в базе данных
			document::update($arangodb->session, $market); */
		} else	if (
			$market = collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN markets FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session,	'markets', [
						'id' => $_row['id'] ?? '',
						'type' => $_row['type'] ?? '',
						'name' => [
							'first' => $name[1] ?? $_row['name'] ?? '',
							'second' => $name[0] ?? '',
							'last' => $name[2] ?? ''
						],
						'address' => $_row['address'] ?? '',
						'city' => $city,
						'active' => true
					])
				)
			)
		) {
			// Не найдена запись магазина (строки) в базе данных и была создана

			// Создание аккаунта
			$account = document::write($arangodb->session,	'account', [
				'type' => 'market',
				'name' => [
					'first' => $name[1] ?? $_row['name'] ?? '',
					'second' => $name[0] ?? '',
					'last' => $name[2] ?? ''
				],
				'number' => convertNumber($_row['number'] ?? ''),
				'active' => true
			]);

			// Подключение сотрудника к аккаунту. Создание ребра: account -> market
			document::write(
				$arangodb->session,
				'account_edge_market',
				['_from' => $account, '_to' => $market->getId()]
			);

			/* // Реинициализация строки с новыми данными по ссылке (приоритет из Google Sheets)
			$row = $row->set((new Flow())->read(From::array([init([
				'id' => $_row['id'] ?? '',
				'type' => $_row['type'] ?? '',
				'director' => $_row['director'] ?? '',
				'address' => $_row['address'] ?? '',
			], true)]))->fetch(1)[0]->get('row')); */
		} else return;
	else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require(__DIR__ . '/../settings/markets/google.php'), true);
$document = require(__DIR__ . '/../settings/markets/document.php');
$sheets = require(__DIR__ . '/../settings/markets/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);
$api = new Sheets($client);

foreach ($sheets as $sheet) {
	$rows = (new Flow())->read(new GoogleSheetExtractor($api, $document, new Columns($sheet, 'A', 'D'), true, 1000, 'row'));

	$i = 1;
	foreach ($rows->fetch(5000) as $row) {
		++$i;
		$buffer = $row;
		sync($row, $sheet);
		if ($buffer !== $row) {
			/* $api->spreadsheets_values->update(
				$document,
				"$sheet!A$i:D$i",
				new ValueRange(['values' => [array_values($row->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);

			// Ожидание для того, чтобы снизить шанс блокировки от Google
			sleep(3); */
		}
	}
}
