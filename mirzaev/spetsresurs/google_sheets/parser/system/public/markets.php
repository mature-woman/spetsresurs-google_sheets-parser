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

$arangodb = new connection(require '../settings/arangodb.php');

function generateLabel(string $name): string
{
	return match ($name) {
		'id', 'ID', 'ТТ'  => 'id',
		'type', 'ТИП', 'Тип', 'тип' => 'type',
		'director', 'ДИРЕКТОР', 'Директор', 'директор' => 'director',
		'address', 'АДРЕС', 'Адрес', 'адрес' => 'address',
		default => throw new exception("Неизвестный столбец: $name")
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'ID', 'id' => 'ID',
		'ТИП', 'type' => 'ТИП',
		'ДИРЕКТОР', 'director' => 'ДИРЕКТОР',
		'АДРЕС', 'address' => 'АДРЕС',
		default => throw new exception("Неизвестный столбец: $name")
	};
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

	$_row = init($row->entries()->toArray()['row']);

	if ($_row['id'] !== null)
		if (collection::init($arangodb->session, 'markets'))
			if ($market = collection::search($arangodb->session, sprintf("FOR d IN markets FILTER d.id == '%s' RETURN d", $_row['id'])))
				if ($_row === $new = array_diff_key($market->getAll(), ['_key' => true, 'created' => true, 'city' => true]));
				else $row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));
			else if (collection::search($arangodb->session, sprintf("FOR d IN markets FILTER d._id == '%s' RETURN d", document::write($arangodb->session, 'markets', $_row + ['city' => $city]))));
			else throw new exception('Не удалось создать или найти созданного магазина');
		else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require('../settings/markets/google.php'), true);
$document = require('../settings/markets/document.php');
$sheets = require('../settings/markets/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);
$api = new Sheets($client);

foreach ($sheets as $sheet) {
	$rows = (new Flow())->read(new GoogleSheetExtractor($api, $document, new Columns($sheet, 'A', 'D'), true, 1000, 'row'));

	$i = 1;
	foreach ($rows->fetch(100) as $row) {
		++$i;
		$buffer = $row;
		sync($row, $sheet);
		if ($buffer !== $row)
			$api->spreadsheets_values->update(
				$document,
				"$sheet!A$i:D$i",
				new ValueRange(['values' => [array_values($row->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);
	}
}
