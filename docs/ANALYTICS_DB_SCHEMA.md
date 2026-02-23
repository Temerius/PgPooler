# Модель аналитической БД PgPooler

Отдельный экземпляр PostgreSQL для метрик, логов подключений, запросов и событий. Используется ядром (или агентом сбора), REST API и Web GUI.

**Рекомендуемое имя сервиса в Docker:** `pgpooler-analytics`.  
**Имя базы данных:** `pgpooler`.

---

## Подключение к аналитике

Ядро и GUI подключаются к аналитике под пользователем **pgpooler**. Для просмотра данных и отладки заходи под тем же пользователем:

| Параметр   | Значение   |
|-----------|------------|
| **Хост**  | `localhost` (с хоста) или `pgpooler-analytics` (из другого контейнера) |
| **Порт**  | `5434` (на хосте маппится с 5432 в контейнере) |
| **Пользователь** | `pgpooler` |
| **Пароль** | `pgpooler` |
| **База**  | `pgpooler` |

**Пример с хоста:**
```bash
psql -h localhost -p 5434 -U pgpooler -d pgpooler
# пароль: pgpooler
```

**Пример из контейнера (например test или gui):**
```bash
psql -h pgpooler-analytics -p 5432 -U pgpooler -d pgpooler
```

Таблицы лежат в схеме **pgpooler**: `pgpooler.connection_sessions`, `pgpooler.queries`, `pgpooler.events` и т.д.

---

## 1. Назначение таблиц

| Таблица | Назначение | Кто пишет | Для API/GUI |
|---------|------------|-----------|-------------|
| **connection_sessions** | История подключений: кто, откуда, куда, режим, длительность. **Активные сессии** = строки с `disconnected_at IS NULL` | Ядро / collector | Статистика; дашборд «сейчас» (WHERE disconnected_at IS NULL) |
| **queries** | Каждый запрос: текст, время, длительность, строки, объём | Ядро / collector | Топ запросов, медленные, объём данных |
| **query_fingerprints** | Нормализованный текст запроса (для группировки) | При вставке в queries | Агрегаты «один запрос — много вызовов» |
| **pool_snapshots** | Снимки пулов раз в N секунд: idle/active/waiting | Ядро / collector | Графики загрузки пулов |
| **events** | Аудит: connect, disconnect, kill, block, reload, error | Ядро / collector | Журнал событий, фильтры |
| **blocked_users** | Заблокированные пользователи (текущее состояние + история) | Ядро / API | Список блокировок, разблокировка |

Ниже — схема с типами и комментариями.

---

## 2. Таблицы (DDL)

### 2.1. connection_sessions — история подключений

Кто подключился, откуда, под каким пользователем, в какую БД, на какой бэкенд, в каком режиме пула, когда отключился, сколько запросов и байт.

```sql
CREATE SCHEMA IF NOT EXISTS pgpooler;

CREATE TABLE pgpooler.connection_sessions (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          INT NOT NULL,              -- id сессии в ядре (worker-scoped)
    worker_id           SMALLINT,                 -- какой воркер обслуживал (если есть)

    -- Клиент
    client_addr         INET,                     -- IP клиента (NULL если неизвестно)
    client_port         INT,                      -- порт клиента (опционально)

    -- Учётные данные и маршрут
    username            TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    backend_name        TEXT NOT NULL,            -- куда маршрутизировали (primary, replica, ...)
    pool_mode           TEXT NOT NULL             -- 'session' | 'transaction' | 'statement'
        CHECK (pool_mode IN ('session', 'transaction', 'statement')),
    application_name    TEXT,                     -- из StartupMessage (опционально)

    -- Время жизни
    connected_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    disconnected_at     TIMESTAMPTZ,              -- NULL пока сессия активна
    duration_sec        NUMERIC(12, 3),          -- заполняется при отключении

    -- Причина отключения (для фильтров и отчётов)
    disconnect_reason   TEXT,                    -- 'client_close' | 'kill' | 'timeout' | 'error' | 'pool_full' | NULL

    -- Мета
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_connection_sessions_connected_at ON pgpooler.connection_sessions (connected_at);
CREATE INDEX idx_connection_sessions_username ON pgpooler.connection_sessions (username);
CREATE INDEX idx_connection_sessions_backend ON pgpooler.connection_sessions (backend_name);
CREATE INDEX idx_connection_sessions_disconnected_at ON pgpooler.connection_sessions (disconnected_at) WHERE disconnected_at IS NOT NULL;
-- Активные сессии: SELECT * FROM pgpooler.connection_sessions WHERE disconnected_at IS NULL
CREATE INDEX idx_connection_sessions_active ON pgpooler.connection_sessions (connected_at) WHERE disconnected_at IS NULL;
```

**Трафик и число запросов по сессии** не храним в `connection_sessions` — считаем в отчётах из `queries`: `SUM(bytes_to_backend + bytes_from_backend)`, `COUNT(*)` по `connection_session_id`. Так избегаем рассинхрона и дублирования.

### 2.2. queries — проходящие запросы

Каждый запрос, прошедший через пулер: привязка к сессии, текст (или ссылка на fingerprint), время, длительность, строки, объём.

```sql
CREATE TABLE pgpooler.queries (
    id                  BIGSERIAL PRIMARY KEY,
    connection_session_id BIGINT,                 -- связь с connection_sessions.id (если храним историю сессий)
    session_id          INT NOT NULL,            -- session_id в ядре на момент запроса
    fingerprint_id      BIGINT,                  -- ссылка на query_fingerprints (нормализованный текст)

    username            TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    backend_name        TEXT NOT NULL,
    application_name    TEXT,                     -- из контекста сессии (StartupMessage)

    -- Запрос
    query_text          TEXT,                    -- сырой текст (можно обрезать до N символов)
    query_text_length   INT,                     -- длина полного текста (если обрезали)

    -- Время и длительность
    started_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at         TIMESTAMPTZ,
    duration_ms         NUMERIC(12, 2),         -- миллисекунды (заполняется по ReadyForQuery)

    -- Результат (из CommandComplete или парсинга ответа)
    command_type        TEXT,                   -- 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'OTHER' | ...
    rows_affected       BIGINT,                 -- для INSERT/UPDATE/DELETE — число строк
    rows_returned       BIGINT,                 -- для SELECT — число строк (если парсим CommandComplete "SELECT N")

    -- Объём данных (если считаем по протоколу)
    bytes_to_backend    BIGINT DEFAULT 0,        -- байт отправлено на бэкенд (этот запрос)
    bytes_from_backend  BIGINT DEFAULT 0,       -- байт получено от бэкенда (ответ)

    -- Ошибка (если запрос завершился с ErrorResponse)
    error_sqlstate      CHAR(5),
    error_message       TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_queries_started_at ON pgpooler.queries (started_at);
CREATE INDEX idx_queries_username ON pgpooler.queries (username);
CREATE INDEX idx_queries_backend ON pgpooler.queries (backend_name);
CREATE INDEX idx_queries_fingerprint ON pgpooler.queries (fingerprint_id);
CREATE INDEX idx_queries_duration ON pgpooler.queries (duration_ms) WHERE duration_ms IS NOT NULL;
```

### 2.3. query_fingerprints — нормализованный текст запроса

Один запрос «по смыслу» (без литералов) — для агрегации: сколько раз вызывался, суммарное время, среднее время.

```sql
CREATE TABLE pgpooler.query_fingerprints (
    id                  BIGSERIAL PRIMARY KEY,
    fingerprint         TEXT NOT NULL,          -- нормализованный запрос (например, числа → ?)
    fingerprint_hash    BYTEA,                  -- или hash для быстрого поиска (md5/sha256)
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fingerprint_hash)                    -- если храним hash; иначе UNIQUE (fingerprint)
);
```

Опционально: вместо отдельной таблицы можно хранить только `fingerprint` в `queries` и агрегировать по нему в представлениях.

### 2.4. pool_snapshots — снимки пулов

Периодические снимки (раз в 10–60 сек): по каждому (backend, user, database) — сколько в пуле idle, сколько занято, сколько ждёт.

```sql
CREATE TABLE pgpooler.pool_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    backend_name        TEXT NOT NULL,
    username            TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    pool_mode           TEXT NOT NULL,

    pool_size           INT NOT NULL,           -- лимит пула (pool_size из конфига)
    idle_count          INT NOT NULL DEFAULT 0, -- соединений в пуле (свободных)
    active_count        INT NOT NULL DEFAULT 0, -- соединений в работе
    waiting_count       INT NOT NULL DEFAULT 0, -- клиентов в очереди ожидания
    avg_wait_ms         NUMERIC(12, 2)          -- среднее время ожидания в очереди (если считаем)
);

CREATE INDEX idx_pool_snapshots_snapshot_at ON pgpooler.pool_snapshots (snapshot_at);
CREATE INDEX idx_pool_snapshots_backend ON pgpooler.pool_snapshots (backend_name, snapshot_at);
```

### 2.5. events — журнал событий

Аудит: каждое значимое действие (подключение, отключение, kill, блокировка пользователя, reload конфига, ошибка).

```sql
CREATE TABLE pgpooler.events (
    id                  BIGSERIAL PRIMARY KEY,
    event_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    event_type          TEXT NOT NULL,          -- 'connect' | 'disconnect' | 'kill' | 'block_user' | 'unblock_user' | 'config_reload' | 'error' | 'pool_full'
    severity            TEXT,                   -- 'info' | 'warn' | 'error'

    username            TEXT,
    database_name       TEXT,
    backend_name        TEXT,
    session_id          INT,
    client_addr         INET,

    message             TEXT,                  -- человекочитаемое описание
    details             JSONB,                 -- произвольные данные (reason, config_path, error_code, ...)

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_events_event_at ON pgpooler.events (event_at);
CREATE INDEX idx_events_event_type ON pgpooler.events (event_type);
CREATE INDEX idx_events_username ON pgpooler.events (username);
CREATE INDEX idx_events_details_gin ON pgpooler.events USING GIN (details);
```

### 2.6. blocked_users — заблокированные пользователи

Текущее состояние: кто заблокирован, когда, кем/почему. Для истории можно дублировать в `events` или вести отдельную таблицу `blocked_users_history`.

```sql
CREATE TABLE pgpooler.blocked_users (
    id                  SERIAL PRIMARY KEY,
    username            TEXT NOT NULL UNIQUE,
    blocked_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    reason              TEXT,                   -- причина (ручная блокировка, политика, ...)
    blocked_by          TEXT,                   -- 'admin' | 'api' | 'config' | ...
    unblocked_at        TIMESTAMPTZ,            -- NULL = пока заблокирован
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_blocked_users_username ON pgpooler.blocked_users (username);
CREATE INDEX idx_blocked_users_unblocked ON pgpooler.blocked_users (unblocked_at) WHERE unblocked_at IS NULL;
```

### 2.7. Активные сессии (без отдельной таблицы)

**Отдельная таблица не нужна.** Пока сессия активна, в `connection_sessions` у неё `disconnected_at IS NULL`. Дашборд «кто сейчас подключён» — выборка по этому условию:

```sql
SELECT * FROM pgpooler.connection_sessions WHERE disconnected_at IS NULL;
```

Для кнопки kill API использует `session_id` + `worker_id` из этой же строки (или получает живой список из ядра по REST). Состояние «idle/active», текущий запрос — либо отдаёт ядро через API (оно держит сессии в памяти), либо в аналитике остаётся только факт «подключён» без детализации по состоянию.

---

## 3. Что ещё можно собирать (по максимуму)

- **Трафик и число запросов по сессии:** только расчёт из `queries` (SUM bytes, COUNT по `connection_session_id`); в `connection_sessions` отдельных полей нет.
- **Трафик по запросу:** `bytes_to_backend`, `bytes_from_backend` в `queries` — если ядро при проксировании считает байты по сообщениям.
- **Строки:** `rows_affected` / `rows_returned` — из парсинга CommandComplete (тег вроде `SELECT 42` или `INSERT 0 1`).
- **Тип команды:** `command_type` — по первому слову запроса или по тегу CommandComplete.
- **Ошибки запросов:** `error_sqlstate`, `error_message` в `queries` — при получении ErrorResponse от бэкенда.
- **Время ожидания в очереди пула:** в `connection_sessions` можно добавить колонку `queue_wait_ms`; в `events` — отдельное событие с длительностью; в `pool_snapshots` — `avg_wait_ms`.
- **Application name:** из StartupMessage — колонка `application_name` добавлена в `connection_sessions` и в `queries`.
- **Частота снимков пула:** 10–60 сек; при большой нагрузке — реже или только при изменении (delta).

Рекомендация: сначала ввести основные таблицы и поля выше; `application_name`, тонкие метрики ожидания и полный парсинг CommandComplete добавлять по мере появления сбора в ядре.

---

## 4. Использование для API и фронта (Python)

- **Дашборд «сейчас»:** `connection_sessions WHERE disconnected_at IS NULL` — список активных сессий; кнопка kill по `session_id` + `worker_id` (или живой список из API ядра).
- **Статистика подключений:** агрегаты по `connection_sessions`: по user, backend, client_addr, pool_mode, по интервалам (час/день); средняя длительность, число подключений.
- **Запросы:** из `queries` — топ по `duration_ms`, по `rows_returned`/`rows_affected`, по `username`/`backend_name`; фильтр по времени.
- **Агрегаты по fingerprint:** группировка по `fingerprint_id` или по `query_text` (нормализованный) — count, sum(duration_ms), avg(duration_ms).
- **Пулы:** из `pool_snapshots` — графики idle/active/waiting по времени и по backend.
- **Журнал событий:** выборка из `events` с фильтрами по типу, пользователю, времени.
- **Блокировки:** чтение/обновление `blocked_users`; разблокировка = установка `unblocked_at`.

Имена таблиц и полей согласованы с этой схемой, чтобы API и фронт могли опираться на один и тот же документ.

---

## 5. Имя сервиса и базы

| Где | Имя | Пример |
|-----|-----|--------|
| Docker Compose (сервис) | **pgpooler-analytics** | `pgpooler-analytics:5432` |
| Имя БД внутри PostgreSQL | **pgpooler** | `psql -h pgpooler-analytics -U ... -d pgpooler` |
| Схема внутри БД | **pgpooler** | `pgpooler.connection_sessions` |

Так и в дипломе можно назвать: «база аналитики PgPooler (сервис pgpooler-analytics, БД pgpooler)».

---

## 6. Миграции

Имеет смысл хранить DDL в репозитории, например:

- `migrations/001_init_analytics_schema.sql` — создание схемы и всех таблиц выше.

При необходимости добавить нумерованные миграции (002, 003, …) для изменений схемы без пересоздания БД.

---

## 7. Итого сущностей и полнота модели

**Сущностей (таблиц) — 6:**

| № | Таблица | Назначение |
|---|---------|------------|
| 1 | **connection_sessions** | Подключения: кто, откуда, куда, режим, время жизни, причина отключения. Активные = `disconnected_at IS NULL`. Трафик и число запросов — из `queries` по `connection_session_id`. |
| 2 | **queries** | Каждый запрос: сессия, текст, время, длительность, строки, байты, тип команды, ошибка. |
| 3 | **query_fingerprints** | Нормализованный текст запроса для группировки (один запрос — много вызовов). |
| 4 | **pool_snapshots** | Снимки пулов: idle/active/waiting по (backend, user, database) раз в N секунд. |
| 5 | **events** | Аудит: connect, disconnect, kill, block, reload, error (event_type, details JSONB). |
| 6 | **blocked_users** | Кто заблокирован, когда, причина; `unblocked_at IS NULL` = ещё заблокирован. |

**Учтено:**

- Статистика подключений — кто, откуда, куда, режим, длительность, причина отключения (`connection_sessions`).
- Запросы — каждый запрос, время, длительность, объём (строки, байты), тип команды, ошибки (`queries`); агрегаты по сессии считаются из `queries` по `connection_session_id`.
- Агрегация запросов по смыслу — `query_fingerprints` + `fingerprint_id` в `queries`.
- Загрузка пулов во времени — `pool_snapshots`.
- Журнал событий и аудит — `events`.
- Блокировки пользователей — `blocked_users`.
- Активные сессии — без отдельной таблицы, выборка `connection_sessions WHERE disconnected_at IS NULL`.
- Application name — колонки в `connection_sessions` и `queries`.

**Опционально на будущее:** колонка `queue_wait_ms` в `connection_sessions` или в `events` (время ожидания в очереди пула), если ядро будет это отдавать.
