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
		'created_in_sheets', 'Отметка времени' => 'created_in_sheets',
		'updated_in_sheets', 'время последнего изменения' => 'updated_in_sheets',
		'date', 'дата заявки' => 'date',
		'market', '№ магазина' => 'market',
		'type', 'формат' => 'type',
		'address', 'адрес' => 'address',
		'worker', 'Код сотрудника (000000)' => 'worker',
		'name', 'ФИО' => 'name',
		'work', 'Вид работ' => 'work',
		'start', 'Время начала заявки' => 'start',
		'end', 'Время окончания заявки' => 'end',
		'hours', 'Количество часов по заявке' => 'hours',
		'tax', 'ИНН' => 'tax',
		'confirmed', 'подтверждение' => 'confirmed',
		'commentary', 'примечание от ТТ' => 'commentary',
		'response', 'ответ от контрагента' => 'response',
		'_id', 'ID'  => '_id',
		default => $name
	};
}

function degenerateLabel(string $name): string
{
	return match ($name) {
		'Отметка времени', 'created_in_sheets' => 'Отметка времени',
		'время последнего изменения', 'updated_in_sheets' => 'время последнего изменения',
		'дата заявки', 'date' => 'дата заявки',
		'№ магазина', 'market' => '№ магазина',
		'формат', 'type' => 'формат',
		'адрес', 'address' => 'адрес',
		'Код сотрудника (000000)', 'worker' => 'Код сотрудника (000000)',
		'ФИО', 'name' => 'ФИО',
		'Вид работ', 'work' => 'Вид работ',
		'Время начала заявки', 'start' => 'Время начала заявки',
		'Время окончания заявки', 'end' => 'Время окончания заявки',
		'Количество часов по заявке', 'hours' => 'Количество часов по заявке',
		'ИНН', 'tax' => 'ИНН',
		'подтверждение', 'confirmed' => 'подтверждение',
		'примечание от ТТ', 'commentary' => 'примечание от ТТ',
		'ответ от контрагента', 'response' => 'ответ от контрагента',
		'ID', '_id' => 'ID',
		default => $name
	};
}

function filterWorker(?string $worker): string
{
	global $arangodb;

	return match ($worker) {
		'Отмена', 'отмена', 'ОТМЕНА' => 'Отмена',
		'', 0, 00, 000, 0000, 00000, 000000, 0000000, 00000000, 000000000, 0000000000 => '',
		default => (function () use ($worker, $arangodb) {
			return $worker;
			/* if (
				collection::init($arangodb->session, 'workers')
				&& collection::search(
					$arangodb->session,
					sprintf(
						"FOR d IN workers FILTER d.id == '%s' RETURN d",
						$worker
					)
				)
			) return $worker;
			else return $worker; */
		})()
	};
}

function init(array $row, bool $reverse = false): array
{
	$buffer = [];

	foreach ($row as $key => $value) $buffer[(($reverse ? 'de' : null) . 'generateLabel')($key)] = $value;

	return $buffer;
}


function sync(int $_i, Row &$row, ?array $raw = null): void
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
					'created_in_sheets' => $work->created_in_sheets,
					'date' => $work->date,
					'worker' => $work->worker,
					'name' => $work->name,
					'work' => $work->work,
					'start' => $work->start,
					'end' => $work->end,
					'hours' => $work->hours,
					'market' => $work->market,
					'type' => $work->type,
					'address' => $work->address,
					'confirmed' => $work->confirmed,
					'commentary' => $work->commentary,
					'response' => $work->response,
					'updated_in_sheets' => $work->updated_in_sheets,
					'tax' => $work->tax,
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
							// Инициализирована инстанция документа в базе данных нового мазагина

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
			&& ($market = collection::search($arangodb->session,	sprintf("FOR d IN markets FILTER d.id == '%s' RETURN d", $_row['imported_market'] ?? $_row['market'])))
			&& $work = collection::search(
				$arangodb->session,
				sprintf(
					"FOR d IN works FILTER d._id == '%s' RETURN d",
					document::write($arangodb->session,	'works', [
						'created_in_sheets' => $raw[0] ?? '',
						'date' => $raw[1] ?? '',
						'worker' => filterWorker($_row['worker'] ?? ''),
						'name' => "=ЕСЛИ(СОВПАД(\$A$_i;\"\");\"\"; ЕСЛИ( НЕ(СОВПАД(IFNA(ВПР(\$C$_i;part_import_KRSK!\$R$2:\$R$4999;1;);\"\");\$C$_i)); ЕСЛИ((СОВПАД(IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$L\$4999;4);\"\");\"\")); IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$J\$4999;2;); \"Сотрудник не назначен\"); \"УВОЛЕН (В списке работающих)\"); \"УВОЛЕН (В списке уволенных)\"))",
						'work' => $_row['work'] ?? '',
						'start' => $raw[5] ?? '',
						'end' => $raw[6] ?? '',
						'hours' => $_row['hours'] ?? '',
						'market' => $_row['market'] ?? '',
						'type' => "=ЕСЛИ(СОВПАД(A$_i;\"\");\"\"; IFNA(ВПР(I$_i;part_import_KRSK!\$B\$2:\$E\$15603;2;);\"Нет в базе\"))",
						'address' => "=ЕСЛИ(СОВПАД(A$_i;\"\");\"\"; IFNA(ВПР(I$_i;part_import_KRSK!\$B\$2:\$E\$15603;4;);\"Нет в базе\"))",
						'confirmed' => $_row['confirmed'] ?? '',
						'commentary' => $_row['commentary'] ?? '',
						'response' => $_row['response'] ?? '',
						'updated_in_sheets' => $raw[14] ?? '',
						'tax' => "=ЕСЛИ(СОВПАД(\$A$_i;\"\");\"\"; IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$K\$5000;3;); IFNA(ВПР(\$C$_i;part_import_KRSK!\$R\$2:\$T\$5000;3;);\"000000000000\")))",
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
				'created_in_sheets' => $raw[0] ?? '',
				'date' => $raw[1] ?? '',
				'worker' => filterWorker($_row['worker'] ?? ''),
				'name' => "=ЕСЛИ(СОВПАД(\$A$_i;\"\");\"\"; ЕСЛИ( НЕ(СОВПАД(IFNA(ВПР(\$C$_i;part_import_KRSK!\$R$2:\$R$4999;1;);\"\");\$C$_i)); ЕСЛИ((СОВПАД(IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$L\$4999;4);\"\");\"\")); IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$J\$4999;2;); \"Сотрудник не назначен\"); \"УВОЛЕН (В списке работающих)\"); \"УВОЛЕН (В списке уволенных)\"))",
				'work' => $_row['work'] ?? '',
				'start' => $raw[5] ?? '',
				'end' => $raw[6] ?? '',
				'hours' => $_row['hours'] ?? '',
				'market' => $_row['market'] ?? '',
				'type' => "=ЕСЛИ(СОВПАД(A$_i;\"\");\"\"; IFNA(ВПР(I$_i;part_import_KRSK!\$B\$2:\$E\$15603;2;);\"Нет в базе\"))",
				'address' => "=ЕСЛИ(СОВПАД(A$_i;\"\");\"\"; IFNA(ВПР(I$_i;part_import_KRSK!\$B\$2:\$E\$15603;4;);\"Нет в базе\"))",
				'confirmed' => $_row['confirmed'] ?? '',
				'commentary' => $_row['commentary'] ?? '',
				'response' => $_row['response'] ?? '',
				'updated_in_sheets' => $raw[14] ?? '',
				'tax' => "=ЕСЛИ(СОВПАД(\$A$_i;\"\");\"\"; IFNA(ВПР(\$C$_i;part_import_KRSK!\$I\$2:\$K\$5000;3;); IFNA(ВПР(\$C$_i;part_import_KRSK!\$R\$2:\$T\$5000;3;);\"000000000000\")))",
				'_id' => $_row['_id'] ?? ''
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

	// Инициализация инстанций Flow для Google Sheet API
	$formulas = (new Flow())->read(new GoogleSheetExtractor($sheets, $document, new Columns($sheet, 'A', 'Q'), true, 1000, 'row', ['valueRenderOption' => 'FORMULA']));
	$rows = $sheets->spreadsheets_values->get($document, "$sheet!A:Q");

	// Инициализация счётчика итераций
	$i = 1;

	foreach ($formulas->fetch(50000) as $formula) {

		// Перебор строк

		// Запись счётчика
		++$i;

		// Инициализация буфера строки
		$buffer = $formula;

		// Синхронизация с базой данных
		sync($i, $formula, $rows[$i - 1] ?? null);

		// Запись изменений строки в Google Sheet
		if ($buffer !== $formula) {
			$sheets->spreadsheets_values->update(
				$document,
				"$sheet!A$i:Q$i",
				new ValueRange(['values' => [array_values($formula->entries()->toArray()['row'])]]),
				['valueInputOption' => 'USER_ENTERED']
			);

			// Ожидание для того, чтобы снизить шанс блокировки от Google
			sleep(3);
		}
	}
}
