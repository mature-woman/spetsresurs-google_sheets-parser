<?php

// Фреймворк ArangoDB
use mirzaev\arangodb\connection,
	mirzaev\arangodb\collection,
	mirzaev\arangodb\document;

// Библиотека для ArangoDB
use ArangoDBClient\Document as _document;

// Фреймворк для Google Sheets
use Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor,
	Flow\ETL\Adapter\GoogleSheet\Columns,
	Flow\ETL\Flow,
	Flow\ETL\Row,
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
		'id', 'ID' => 'id',
		'name', 'ФИО' => 'name',
		'phone', 'Номер' => 'phone',
		'birth', 'Дата рождения' => 'birth',
		'address', 'Регистрация' => 'address',
		'commentary', 'Комментарий' => 'commentary',
		'activity', 'Работа' => 'activity',
		'passport', 'Паспорт' => 'passport',
		'issued', 'Выдан' => 'issued',
		'department', 'Подразделение' => 'department',
		'hiring', 'Нанят' => 'hiring',
		'district', 'Район', 'район' => 'district',
		'requisites', 'Реквизиты', 'реквизиты' => 'requisites',
		'fired', 'Уволен' => 'fired',
		'payment', 'Оплата', 'оплата' => 'payment',
		'tax', 'ИНН', 'инн' => 'tax',
		default => $name
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'ID', 'id' => 'ID',
		'ФИО', 'name' => 'ФИО',
		'Номер', 'phone' => 'Номер',
		'Дата рождения', 'birth' => 'Дата рождения',
		'Регистрация', 'address' => 'Регистрация',
		'Комментарий', 'commentary' => 'Комментарий',
		'Работа', 'activity' => 'Работа',
		'Паспорт', 'passport' => 'Паспорт',
		'Выдан', 'issued' => 'Выдан',
		'Подразделение', 'department' => 'Подразделение',
		'Нанят', 'hiring' => 'Нанят',
		'Район', 'district' => 'Район',
		'Реквизиты', 'requisites' => 'Реквизиты',
		'Уволен', 'fired' => 'Дата увольнения',
		'Оплата', 'payment' => 'Оплата',
		'ИНН', 'tax' => 'ИНН',
		default => $name
	};
}

function convertNumber(string $number): string
{

	// Очистка всего кроме цифр, а потом поиск 10 первых чисел (без восьмёрки)
	preg_match('/^8(\d{10})/', preg_replace("/[^\d]/", "", $number), $matches);

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


function sync(Row &$row, string $city = 'Красноярск', array $formulas = []): void
{
	global $arangodb;

	// Инициализация строки в Google Sheet
	$_row = init($row->toArray()['row']);

	if (collection::init($arangodb->session, 'workers'))
		if (!empty($_row['id']) && $worker = collection::search($arangodb->session, sprintf("FOR d IN workers FILTER d.id == '%s' RETURN d", $_row['id']))) {
			// Найдена запись работника (строки) в базе данных и включен режим перезаписи (приоритет - google sheets)

			if ($worker->transfer_to_sheets) {
				// Запрошен форсированный перенос данных из базы данных в таблицу

				// Инициализация данных для записи в таблицу
				$new = [
					'id' => $worker->id ?? '',
					'name' => $worker->name ?? '',
					'phone' => convertNumber($worker->phone ?? ''),
					'birth' => $worker->birth ?? '',
					'address' => $worker->address ?? '',
					'commentary' => $worker->commentary ?? '',
					'activity' => $worker->activity ?? '',
					'passport' => $worker->passport ?? '',
					'issued' => $worker->issued ?? '',
					'department' => $worker->department ?? '',
					'hiring' => $worker->hiring ?? '',
					'district' => $worker->district ?? '',
					'requisites' => $worker->requisites ?? '',
					'fired' => $worker->fired ?? '',
					'payment' => $worker->payment ?? '',
					'tax' => $worker->tax ?? '',
				];

				// Замена NULL на пустую строку
				foreach ($new as $key => &$value) if ($value === null) $value = '';

				// Реинициализация строки с новыми данными по ссылке (приоритет из базы данных)
				if ($_row !== $new) $row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));

				// Деактивация форсированного трансфера
				$worker->transfer_to_sheets = false;
			} else {
				// Перенос изменений из Google Sheet в инстанцию документа в базе данных

				// Реинициализация данных в инстанции документа в базе данных с данными из Google Sheet
				foreach ($worker->getAll() as $key => $value) {
					// Перебор всех записанных значений в инстанции документа в базе данных

					// Конвертация
					$worker->{$key} = $_row[$key] ?? $value;
				}

				if (strlen($formulas[2]) < 12) {
					// Не конвертирован номер

					// Инициализация номера
					$number = convertNumber($_row['phone'] ?? '');

					// Реинициализация строки с новыми данными по ссылке (приоритет из Google Sheets)
					$row = $row->set((new Flow())->read(From::array([init([
						'id' => $_row['id'] ?? '',
						'name' => $_row['name'] ?? '',
						'phone' =>  "=HYPERLINK(\"https://call.ctrlq.org/+$number\"; \"$number\")",
						'birth' => $_row['birth'] ?? '',
						'address' => $_row['address'] ?? '',
						'commentary' => $_row['commentary'] ?? '',
						'activity' => $_row['activity'] ?? '',
						'passport' => $_row['passport'] ?? '',
						'issued' => $_row['issued'] ?? '',
						'department' => $_row['department'] ?? '',
						'hiring' => $_row['hiring'] ?? '',
						'district' => $_row['district'] ?? '',
						'requisites' => $_row['requisites'] ?? '',
						'fired' => $_row['fired'] ?? '',
						'payment' => $_row['payment'] ?? '',
						'tax' => $_row['tax'] ?? '',
					], true)]))->fetch(1)[0]->get('row'));
				}
			}

			// Обновление инстанции документа в базе данных
			document::update($arangodb->session, $worker);

			// Подключение к чат-роботам
			connectAll($worker);
		} else	if (
			!empty($_row['id'])
			&& !empty($_row['phone'])
			&& $worker = collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN workers FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session,	'workers', [
						'id' => $_row['id'] ?? '',
						'name' => $_row['name'] ?? '',
						'phone' => convertNumber($_row['phone'] ?? ''),
						'birth' => $_row['birth'] ?? '',
						'address' => $_row['address'] ?? '',
						'commentary' => $_row['commentary'] ?? '',
						'activity' => $_row['activity'] ?? '',
						'passport' => $_row['passport'] ?? '',
						'issued' => $_row['issued'] ?? '',
						'department' => $_row['department'] ?? '',
						'hiring' => $_row['hiring'] ?? '',
						'district' => $_row['district'] ?? '',
						'requisites' => $_row['requisites'] ?? '',
						'fired' => $_row['fired'] ?? '',
						'payment' => $_row['payment'] ?? '',
						'tax' => $_row['tax'] ?? '',
						'city' => $city,
						'transfer_to_sheets' => false
					])
				)
			)
		) {
			// Не найдена запись работника (строки) в базе данных и была создана

			// Инициализация номера
			$number = convertNumber($_row['phone'] ?? '');

			// Реинициализация строки с новыми данными по ссылке (приоритет из Google Sheets)
			$row = $row->set((new Flow())->read(From::array([init([
				'id' => $_row['id'] ?? '',
				'name' => $_row['name'] ?? '',
				'phone' =>  "=HYPERLINK(\"https://call.ctrlq.org/+$number\"; \"$number\")",
				'birth' => $_row['birth'] ?? '',
				'address' => $_row['address'] ?? '',
				'commentary' => $_row['commentary'] ?? '',
				'activity' => $_row['activity'] ?? '',
				'passport' => $_row['passport'] ?? '',
				'issued' => $_row['issued'] ?? '',
				'department' => $_row['department'] ?? '',
				'hiring' => $_row['hiring'] ?? '',
				'district' => $_row['district'] ?? '',
				'requisites' => $_row['requisites'] ?? '',
				'fired' => $_row['fired'] ?? '',
				'payment' => $_row['payment'] ?? '',
				'tax' => $_row['tax'] ?? '',
			], true)]))->fetch(1)[0]->get('row'));

			// Подключение к чат-роботам
			connectAll($worker);
		} else return;
	else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require(__DIR__ . '/../settings/workers/google.php'), true);
$document = require(__DIR__ . '/../settings/workers/document.php');
$sheets = require(__DIR__ . '/../settings/workers/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);

foreach ($sheets as $sheet) {
	// Перебор таблиц

	// Инициализация обработчика таблиц
	$sheets = new Sheets($client);

	// Инициализация инстанции Flow для Google Sheet API
	$rows = (new Flow())->read(new GoogleSheetExtractor($sheets, $document, new Columns($sheet, 'A', 'P'), true, 1000, 'row'));

	// Инициализация счётчика итераций
	$i = 1;

	$formulas = $sheets->spreadsheets_values->get($document, "$sheet!A:P", ['valueRenderOption' => 'FORMULA']) ?? null;

	if ($formulas === null) continue;

	foreach ($rows->fetch(5000) as $row) {
		// Перебор строк

		// Запись счётчика
		++$i;

		// Инициализация буфера строки
		$buffer = $row;

		// Синхронизация с базой данных
		sync($row, $sheet, $formulas[$i - 1]);

		// Запись изменений строки в Google Sheet
		if ($buffer !== $row) {
			$sheets->spreadsheets_values->update(
				$document,
				"$sheet!A$i:P$i",
				new ValueRange(['values' => [array_values($row->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);

			// Ожидание для того, чтобы снизить шанс блокировки от Google
			sleep(3);
		}
	}
}
