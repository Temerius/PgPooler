# План: pool_snapshots

## Цель

Периодически (раз в N секунд) делать снимок состояния пулов и писать в таблицу `pgpooler.pool_snapshots`: по каждому (backend, user, database) — сколько соединений в пуле (idle), сколько занято (active), сколько клиентов ждёт в очереди (waiting), лимит пула (pool_size) и режим (pool_mode). Это нужно для графиков загрузки пулов во времени.

## Схема (уже есть в 001_init_analytics_schema.sql)

- `snapshot_at` — момент снимка
- `backend_name`, `username`, `database_name`, `pool_mode`
- `pool_size` — лимит из конфига/роутинга
- `idle_count` — соединений в пуле (свободных)
- `active_count` — соединений в работе (взяты из пула, ещё не возвращены)
- `waiting_count` — клиентов в очереди ожидания (пул полный, ждут освобождения)
- `avg_wait_ms` — пока NULL (время ожидания в очереди не считаем)

## Откуда брать данные

1. **idle_count** — из `BackendConnectionPool`: по ключу (backend_name, user, database) размер вектора в `idle_`.
2. **active_count** — число соединений, взятых из пула и ещё не возвращённых. Сейчас пул это не хранит → нужно в `BackendConnectionPool` вести счётчик «выдано» по ключу: +1 в `take()`, −1 в `put()` (и при `take_one_to_close` / `take_one_expired` не трогать — это про idle).
3. **waiting_count** — из `ConnectionWaitQueue`: по (backend_name, user, database) посчитать количество элементов в `waiters_` с таким ключом.
4. **pool_size**, **pool_mode** — из роутинга: для каждого (user, database) вызывать `resolver(user, database)` и брать `pool_size` и `pool_mode` из `ResolvedBackend` (backend_name в результате должен совпадать с ключом).

## Ключи для снимка

Снимок делаем по всем ключам, которые «имеют смысл»:
- все ключи из пула (idle_): по каждому ключу есть хотя бы idle или active;
- все ключи из очереди ожидания (waiters_).

Объединяем множества ключей из пула и из очереди, по каждому ключу строим одну строку снимка.

## Кто и когда делает снимок

- **Интервал**: конфиг, например `analytics.pool_snapshot_interval_sec` (по умолчанию 30). При 0 — снимки не делать.
- **Где запускать таймер**: в том же event loop, где слушатель (одиночный процесс — в `main` после создания Listener; воркеры — внутри `run_worker`). Добавить `event_new(..., EV_TIMEOUT | EV_PERSIST)` с интервалом N сек.
- **В колбеке таймера** (в потоке event loop):
  1. Вызвать у пула метод снимка: получить список (backend, user, database, idle_count, active_count).
  2. Вызвать у очереди метод: получить (backend, user, database) → waiting_count.
  3. Собрать множество всех ключей (из пула + из очереди).
  4. Для каждого ключа: вызвать `resolver(user, database)` → взять pool_size и pool_mode (и проверить, что backend совпадает).
  5. Сформировать событие (или массив строк) и отправить в analytics: `push_pool_snapshot(...)`.

## Изменения в коде (кратко)

1. **BackendConnectionPool**
   - Добавить `std::map<Key, size_t> checked_out_`; в `take()` увеличивать, в `put()` уменьшать.
   - Добавить метод вида `get_snapshot()` → `std::vector<PoolSnapshotRow>` с полями (backend_name, user, database, idle_count, active_count). Под замком обойти `idle_` и `checked_out_`, выдать все ключи с ненулевыми idle или checked_out.

2. **ConnectionWaitQueue**
   - Добавить метод `get_waiting_counts()` → например `std::map<std::tuple<std::string,std::string,std::string>, size_t>` или структура с (backend, user, db) → count. Обойти `waiters_` и посчитать.

3. **Конфиг**
   - В `AnalyticsConfig` (или рядом): `pool_snapshot_interval_sec` (unsigned, 0 = выключено).

4. **Таймер снимка**
   - В однопроцессном режиме: в `main` после создания Listener добавить таймер на `event_base`, в колбеке — собрать снимок (нужен доступ к connection_pool, wait_queue, resolver, analytics). Передать в колбек контекст (pool, queue, resolver, analytics, interval).
   - В режиме с воркерами: таймер ставить в каждом воркере (у каждого свой pool, queue, resolver, analytics). Тогда в БД будут строки «по воркеру»: один и тот же (backend, user, db) может появиться в нескольких строках с одним snapshot_at (по одной на воркер). Для дашборда можно либо оставить так (фильтр по worker_id, если добавим колонку), либо потом делать агрегацию по snapshot_at и ключу в представлении.

5. **AnalyticsWriter**
   - Новый тип события, например `PoolSnapshotEvent`, с массивом строк: (backend_name, username, database_name, pool_mode, pool_size, idle_count, active_count, waiting_count, avg_wait_ms).
   - В `process_one` для этого события: для каждой строки `INSERT INTO pgpooler.pool_snapshots (...) VALUES (...)`.

## Режим с воркерами

Каждый воркер — отдельный процесс со своим пулом и очередью. У каждого свой таймер и свой AnalyticsWriter. Каждый воркер шлёт свои строки снимка. В итоге по одному и тому же (backend, user, db) может быть несколько строк с одинаковым snapshot_at (от разных воркеров). Варианты:
- Добавить в таблицу колонку `worker_id` и в снимке передавать номер воркера; дашборд может показывать по воркеру или суммировать.
- Либо не добавлять worker_id и принять, что при нескольких воркерах строки «дублируются» по ключу — тогда агрегация по времени и ключу (SUM idle, SUM active, SUM waiting) в представлении даст суммарную картину.

На первом этапе можно не добавлять worker_id и просто писать снимки как есть; при одном воркере всё однозначно.

## Итог

- Пул: учёт active через `checked_out_` + метод снимка.
- Очередь: метод подсчёта waiting по ключам.
- Таймер в event loop, раз в N сек собираем ключи, дергаем resolver для pool_size/pool_mode, шлём событие в AnalyticsWriter.
- Writer пишет в `pool_snapshots` по одной строке на ключ (и при нескольких воркерах — по одной строке на воркер на ключ).
