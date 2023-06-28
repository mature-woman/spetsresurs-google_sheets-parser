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
		'imported_created_in_sheets', 'Отметка времени' => 'imported_created_in_sheets',
		'imported_date', 'Дата заявки' => 'imported_date',
		'imported_market', 'Ваш магазин' => 'imported_market',
		'imported_worker', 'Требуемый сотрудник' => 'imported_worker',
		'imported_work', 'Вид работы' => 'imported_work',
		'imported_start', 'Начало работы' => 'imported_start',
		'imported_end', 'Конец работы' => 'imported_end',
		'imported_hours', 'Часы работы' => 'imported_hours',
		'created_in_sheets', 'Создано' => 'created_in_sheets',
		'date', 'Дата' => 'date',
		'market', 'Магазин' => 'market',
		'type', 'Тип' => 'type',
		'address', 'Адрес' => 'address',
		'worker', 'Сотрудник' => 'worker',
		'name', 'ФИО' => 'name',
		'work', 'Работа' => 'work',
		'start', 'Начало' => 'start',
		'end', 'Конец' => 'end',
		'hours', 'Часы' => 'hours',
		'tax', 'ИНН' => 'tax',
		'confirmed', 'Подтверждено' => 'confirmed',
		'commentary', 'Комментарий' => 'commentary',
		'response', 'Ответ' => 'response',
		'_id', 'ID'  => '_id',
		default => $name
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'Отметка времени', 'imported_created_in_sheets' => 'Отметка времени',
		'Дата заявки', 'imported_date' => 'Дата заявки',
		'Ваш магазин', 'imported_market' => 'Ваш магазин',
		'Требуемый сотрудник', 'imported_worker' => 'Требуемый сотрудник',
		'Вид работы', 'imported_work' => 'Вид работы',
		'Начало работы', 'imported_start' => 'Начало работы',
		'Конец работы', 'imported_end' => 'Конец работы',
		'Часы работы', 'imported_hours' => 'Часы работы',
		'Создано', 'created_in_sheets' => 'Создано',
		'Дата', 'date' => 'Дата',
		'Магазин', 'market' => 'Магазин',
		'Тип', 'type' => 'Тип',
		'Адрес', 'address' => 'Адрес',
		'Сотрудник', 'worker' => 'Сотрудник',
		'ФИО', 'name' => 'ФИО',
		'Работа', 'work' => 'Работа',
		'Начало', 'start' => 'Начало',
		'Конец', 'end' => 'Конец',
		'Часы', 'hours' => 'Часы',
		'ИНН', 'tax' => 'ИНН',
		'Подтверждено', 'confirmed' => 'Подтверждено',
		'Комментарий', 'commentary' => 'Комментарий',
		'Ответ', 'response' => 'Ответ',
		'ID', '_id' => 'ID',
		default => $name
	};
}

function filterWorker(?string $worker): string
{
	global $arangodb;

	return match ((int) $worker) { 
		'Отмена', 'отмена', 'ОТМЕНА' => 'Отмена',
		'', 0, 00, 000, 0000, 00000, 000000, 0000000, 00000000, 000000000, 0000000000 => '',
		default => (function () use ($worker, $arangodb) {
			if (
				collection::init($arangodb->session, 'workers')
				&& collection::search(
					$arangodb->session,
					sprintf(
						"FOR d IN workers FILTER d.id == '%s' RETURN d",
						$worker
					)
				)
			) return $worker;
			else return '';
		})()
	};
}

function init(array $row, bool $reverse = false): array
{
	$buffer = [];

	foreach ($row as $key => $value) $buffer[(($reverse ? 'de' : null) . 'generateLabel')($key)] = $value;

	return $buffer;
}


function sync(int $_i, Row &$row, array $raw = []): void
{
	global $arangodb;

	// Инициализация строки в Google Sheet
	$_row = init($row->toArray()['row']);

	// Замена пустой строки на NULL (для логических операций)
	foreach ($_row as $key => &$value) if ($value === '') $value = null;
	foreach ($raw as $key => &$value) if ($value === '') $value = null;

	if (collection::init($arangodb->session, 'works'))
		if (!empty($_row['_id']) && $work = collection::search($arangodb->session, sprintf("FOR d IN works FILTER d._id == '%s' RETURN d", $_row['_id']))) {
			// Найдена запись работы (строки) в базе данных 

			if ($work->transfer_to_sheets) {
				// Запрошен форсированный перенос данных из базы данных в таблицу

				// Инициализация данных для записи в таблицу
				$new = [
					'imported_created_in_sheets' => $work->imported_created_in_sheets,
					'imported_date' => $work->imported_date,
					'imported_market' => $work->imported_market,
					'imported_worker' => $work->imported_worker,
					'imported_work' => $work->imported_work,
					'imported_start' => $work->imported_start,
					'imported_end' => $work->imported_end,
					'imported_hours' => $work->imported_hours,
					'created_in_sheets' => $work->created_in_sheets,
					'date' => $work->date,
					'market' => $work->market,
					'type' => $work->type,
					'address' => $work->address,
					'worker' => $work->worker,
					'name' => $work->name,
					'work' => $work->work,
					'start' => $work->start,
					'end' => $work->end,
					'hours' => $work->hours,
					'tax' => $work->tax,
					'confirmed' => $work->confirmed,
					'commentary' => $work->commentary,
					'response' => $work->response,
					'_id' => $work->getId(),
				];

				// Инициализация сотрудника
				if (collection::init($arangodb->session, 'readinesses', true)	&& collection::init($arangodb->session, 'workers'))
					$new['worker'] = collection::search(
						$arangodb->session,
						$worker = sprintf(
							"FOR d IN workers LET e = (FOR e IN readinesses FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
							$_row['_id']
						)
					)?->id;
				else throw new exception('Не удалось инициализировать коллекции');

				// Инициализация магазина
				if (collection::init($arangodb->session, 'requests', true)	&& collection::init($arangodb->session, 'markets'))
					if ($new['market'] = collection::search(
						$arangodb->session,
						sprintf(
							"FOR d IN markets LET e = (FOR e IN requests FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
							$_row['_id']
						)
					)?->id);
					else throw new exception('Не удалось найти магазин');
				else throw new exception('Не удалось инициализировать коллекции');

				// Замена NULL на пустую строку
				foreach ($new as $key => &$value) if ($value === null) $value = '';

				// Реинициализация строки с новыми данными по ссылке (приоритет из базы данных)
				if ($_row !== $new) $row = $row->set((new Flow())->read(From::array([init($new, true)]))->fetch(1)[0]->get('row'));

				// Деактивация форсированного трансфера
				$work->transfer_to_sheets = false;
			} else {
				// Перенос изменений из Google Sheet в инстанцию документа в базе данных

				if (
					collection::init($arangodb->session, 'readinesses', true)	&& collection::init($arangodb->session, 'workers')
					&& ($worker = collection::search(
						$arangodb->session,
						sprintf(
							"FOR d IN workers LET e = (FOR e IN readinesses FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
							$_row['_id']
						)
					))
					&& $_row['worker'] !== $work->worker
				) {
					// Изменён сотрудник (подразумевается, что внутри google sheet)

					if ($readiness = collection::search(
						$arangodb->session,
						sprintf(
							"FOR e IN readinesses FILTER e._from == '%s' && e._to == '%s' LIMIT 1 RETURN e",
							$worker->getId(),
							$_row['_id']
						)
					)) {
						// Инициализировано ребро: worker => work

						if ($_worker = collection::search(
							$arangodb->session,
							sprintf(
								"FOR d IN workers FILTER d.id == '%s' LIMIT 1 RETURN d",
								$_row['worker']
							)
						)) {
							// Инициализирована инстанция документа в базе данных нового работника

							// Реинициализация работника
							$readiness->_from = $_worker->getId();

							// Обновление в базе данных
							document::update($arangodb->session, $readiness);
						}
					}
				}

				if (
					collection::init($arangodb->session, 'requests', true)	&& collection::init($arangodb->session, 'markets')
					&& ($market = collection::search(
						$arangodb->session,
						sprintf(
							"FOR d IN markets LET e = (FOR e IN requests FILTER e._to == '%s' RETURN e._from)[0] FILTER d._id == e RETURN d",
							$_row['_id']
						)
					))
					&& $_row['market'] !== $work->market
				) {
					// Изменён магазин (подразумевается, что внутри google sheet)

					if ($request = collection::search(
						$arangodb->session,
						sprintf(
							"FOR e IN requests FILTER e._from == '%s' && e._to == '%s' LIMIT 1 RETURN e",
							$market->getId(),
							$_row['_id']
						)
					)) {
						// Инициализировано ребро: market => work

						if ($_market = collection::search(
							$arangodb->session,
							sprintf(
								"FOR d IN markets FILTER d.id == '%s' LIMIT 1 RETURN d",
								$_row['market']
							)
						)) {
							// Инициализирована инстанция документа в базе данных нового магазина

							// Реинициализация магазина
							$request->_from = $_market->getId();

							// Обновление в базе данных
							document::update($arangodb->session, $request);
						}
					}
				}

				// Инициализация счётчика итераций
				$i = 0;

				// Реинициализация данных в инстанции документа в базе данных с данными из Google Sheet
				foreach (array_diff_key($work->getAll(), ['_key' => true, 'created' => true]) as $key => $value) {
					// Перебор всех записанных значений в инстанции документа в базе данных

					// Конвертация
					$work->{$key} = is_array($value) ? ['number' => $_row[$key] ?? $value, 'converted' => $raw[$i]] : $_row[$key] ?? $value;

					// Запись в счётчик итераций
					++$i;
				}
			}

			// Обновление инстанции документа в базе данных
			document::update($arangodb->session, $work);
		} else	if (
			(!empty($_row['imported_market']) || !empty($_row['market']))
			&& collection::init($arangodb->session, 'requests', true)	&& collection::init($arangodb->session, 'markets')
			&& ($market = collection::search($arangodb->session,	sprintf("FOR d IN markets FILTER d.id == '%s' RETURN d", $raw[2] ?? $_row[10])))
			&& $work = collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN works FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session,	'works', [
						'imported_created_in_sheets' => $raw[0],
						'imported_date' => $raw[1],
						'imported_market' => $raw[2],
						'imported_worker' => $raw[3],
						'imported_work' => $raw[4],
						'imported_start' => $raw[5],
						'imported_end' => $raw[6],
						'imported_hours' => $raw[7],
						'created_in_sheets' => $raw[0] ?? $raw[8],
						'date' => $raw[1] ?? $raw[9],
						'market' => $raw[2] ?? $raw[10],
						'type' => "=ЕСЛИ(СОВПАД(I$_i;\"\");\"\"; IFNA(ВПР(K$_i;part_import_KRSK!\$B\$2:\$E\$15603;2;);\"Нет в базе\"))",
						'address' => "=ЕСЛИ(СОВПАД(I$_i;\"\");\"\"; IFNA(ВПР(K$_i;part_import_KRSK!\$B\$2:\$E\$15603;4;);\"Нет в базе\"))",
						'worker' => filterWorker($raw[13] ?? $raw[3]),
						'name' => "=ЕСЛИ(СОВПАД(\$I$_i;\"\");\"\"; ЕСЛИ( НЕ(СОВПАД(IFNA(ВПР(\$N$_i;part_import_KRSK!\$R$2:\$R$4999;1;);\"\");\$N$_i)); ЕСЛИ((СОВПАД(IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$L\$4999;4);\"\");\"\")); IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$J\$4999;2;); \"Сотрудник не назначен\"); \"УВОЛЕН (В списке работающих)\"); \"УВОЛЕН (В списке уволенных)\"))",
						'work' => $raw[4] ?? $raw[15],
						'start' => $raw[5] ?? $raw[16],
						'end' => $raw[6] ?? $raw[17],
						'hours' => $raw[7] ?? $raw[18],
						'tax' => "=ЕСЛИ(СОВПАД(\$I$_i;\"\");\"\"; IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$K\$5000;3;); IFNA(ВПР(\$N$_i;part_import_KRSK!\$R\$2:\$T\$5000;3;);\"000000000000\")))",
						'confirmed' => $_row['confirmed'] ?? '',
						'commentary' => $_row['commentary'] ?? '',
						'response' => $_row['response'] ?? '',
						'transfer_to_sheets' => false
					])
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

			// Запись идентификатора только что созданной инстанции документа в базе данных
			$_row['_id'] = $work->getId();

			// Реинициализация строки с новыми данными по ссылке (приоритет из Google Sheets)
			$row = $row->set((new Flow())->read(From::array([init([
				'imported_created_in_sheets' => $raw[0] ?? '',
				'imported_date' => $raw[1] ?? '',
				'imported_market' => $_row['imported_market'] ?? '',
				'imported_worker' => $_row['imported_worker'] ?? '',
				'imported_work' => $_row['imported_work'] ?? '',
				'imported_start' => $raw[5] ?? '',
				'imported_end' => $raw[6] ?? '',
				'imported_hours' => $_row['imported_hours'] ?? '',
				'created_in_sheets' => $raw[0] ?? $raw[8],
				'date' => $raw[1] ?? $raw[9],
				'market' => $raw[2] ?? $raw[10],
				'type' => "=ЕСЛИ(СОВПАД(I$_i;\"\");\"\"; IFNA(ВПР(K$_i;part_import_KRSK!\$B\$2:\$E\$15603;2;);\"Нет в базе\"))",
				'address' => "=ЕСЛИ(СОВПАД(I$_i;\"\");\"\"; IFNA(ВПР(K$_i;part_import_KRSK!\$B\$2:\$E\$15603;4;);\"Нет в базе\"))",
				'worker' => filterWorker($raw[13] ?? $raw[3]),
				'name' => "=ЕСЛИ(СОВПАД(\$I$_i;\"\");\"\"; ЕСЛИ( НЕ(СОВПАД(IFNA(ВПР(\$N$_i;part_import_KRSK!\$R$2:\$R$4999;1;);\"\");\$N$_i)); ЕСЛИ((СОВПАД(IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$L\$4999;4);\"\");\"\")); IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$J\$4999;2;); \"Сотрудник не назначен\"); \"УВОЛЕН (В списке работающих)\"); \"УВОЛЕН (В списке уволенных)\"))",
				'work' => $raw[4] ?? $raw[15],
				'start' => $raw[5] ?? $raw[16],
				'end' => $raw[6] ?? $raw[17],
				'hours' => $raw[7] ?? $raw[18],
				'tax' => "=ЕСЛИ(СОВПАД(\$I$_i;\"\");\"\"; IFNA(ВПР(\$N$_i;part_import_KRSK!\$I\$2:\$K\$5000;3;); IFNA(ВПР(\$N$_i;part_import_KRSK!\$R\$2:\$T\$5000;3;);\"000000000000\")))",
				'confirmed' => $_row['confirmed'] ?? '',
				'commentary' => $_row['commentary'] ?? '',
				'response' => $_row['response'] ?? '',
				'_id' => $_row['_id'] ?? '',
			], true)]))->fetch(1)[0]->get('row'));
		} else return;
	else throw new exception('Не удалось инициализировать коллекцию');
}

$settings = json_decode(require(__DIR__ . '/../settings/works/google.php'), true);
$document = require(__DIR__ . '/../settings/works/document.php');
$sheets = require(__DIR__ . '/../settings/works/sheets.php');

$client = new Client();
$client->setScopes(Sheets::SPREADSHEETS);
$client->setAuthConfig($settings);

foreach ($sheets as $sheet) {
	// Перебор таблиц

	// Инициализация обработчика таблиц
	$sheets = new Sheets($client);

	// Инициализация инстанции Flow для Google Sheet API
	$rows = (new Flow())->read(new GoogleSheetExtractor($sheets, $document, new Columns($sheet, 'A', 'X'), true, 1000, 'row'));

	// Инициализация счётчика итераций
	$i = 1;

	$raws = $sheets->spreadsheets_values->get($document, "$sheet!A:X") ?? null;

	if ($raws === null) continue;

	foreach ($rows->fetch(10000) as $row) {
		// Перебор строк

		// Запись счётчика
		++$i;

		// Инициализация буфера строки
		$buffer = $row;

		// Синхронизация с базой данных
		sync($i, $row, $raws[$i - 1]);

		// Запись изменений строки в Google Sheet
		if ($buffer !== $row) {
			$row = init($row->toArray()['row']);

			$sheets->spreadsheets_values->update(
				$document,
				"$sheet!I$i:X$i",
				new ValueRange(['values' => [array_values(init([
					'created_in_sheets' => $row['created_in_sheets'] ?? '',
					'date' => $row['date'] ?? '',
					'market' => $row['market'] ?? '',
					'type' => $row['type'] ?? '',
					'address' => $row['address'] ?? '',
					'worker' => $row['worker'] ?? '',
					'name' => $row['name'] ?? '',
					'work' => $row['work'] ?? '',
					'start' => $row['start'] ?? '',
					'end' => $row['end'] ?? '',
					'hours' => $row['hours'] ?? '',
					'tax' => $row['tax'] ?? '',
					'confirmed' => $row['confirmed'] ?? '',
					'commentary' => $row['commentary'] ?? '',
					'response' => $row['response'] ?? '',
					'_id' => $row['_id'] ?? '',
				], true))]]),
				['valueInputOption' => 'USER_ENTERED']
			);

			// Ожидание для того, чтобы снизить шанс блокировки от Google
			sleep(3);
		}
	}
}
