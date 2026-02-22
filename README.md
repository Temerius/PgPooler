# PgPooler

**Программное средство управления пулом соединений и маршрутизации запросов в PostgreSQL.**

Аналог [PgBouncer](https://www.pgbouncer.org/) и [Odyssey](https://github.com/yandex/odyssey): прокси между клиентами и одним или несколькими серверами PostgreSQL с пулингом соединений, маршрутизацией по пользователям и веб-интерфейсом для мониторинга и управления.

---

## Содержание

- [Общая архитектура](#общая-архитектура)
- [Режимы пулинга](#режимы-пулинга)
- [Стек технологий](#стек-технологий)
- [Компоненты системы](#компоненты-системы)
- [Конфигурация (XML)](#конфигурация-xml)
- [REST API (Admin)](#rest-api-admin)
- [База аналитики](#база-аналитики)
- [Поток данных](#поток-данных)
- [Отличия от PgBouncer и Odyssey](#отличия-от-pgbouncer-и-odyssey)
- [План реализации](#план-реализации)
- [Модель параллелизма (много клиентов)](docs/CONCURRENCY.md)
- [Протокол PostgreSQL (wire)](src/protocol/README.md)
- [**Формат конфига: маршрутизация и пулы**](docs/CONFIG_FORMAT.md) — YAML, правила, regex/списки/префиксы, режимы пула

---

## Общая архитектура

```
                    Клиенты (psql, приложения)
                           │   │   │   │   │
                           ▼   ▼   ▼   ▼   ▼
                    ┌─────────────────────────┐
                    │     PgPooler Core       │
                    │       (C++17)           │
                    │                         │
                    │  ┌───────────────────┐  │
                    │  │  PG Wire Protocol │  │  ◄── порт 6432
                    │  │    Listener       │  │
                    │  └────────┬──────────┘  │
                    │           │              │
                    │  ┌────────▼──────────┐  │
                    │  │  Authenticator    │  │
                    │  └────────┬──────────┘  │
                    │           │              │
                    │  ┌────────▼──────────┐  │
                    │  │     Router        │  │  ◄── правила из XML
                    │  │  (user → backend) │  │
                    │  └────────┬──────────┘  │
                    │           │              │
                    │  ┌────────▼──────────┐  │
                    │  │  Pool Manager     │  │
                    │  │ ┌──────┐ ┌──────┐ │  │
                    │  │ │Pool 1│ │Pool 2│ │  │
                    │  │ └──┬───┘ └──┬───┘ │  │
                    │  └────┼────────┼─────┘  │
                    │       │        │         │
                    │  ┌────▼────────▼─────┐  │
                    │  │ Metrics Collector  │  │
                    │  └────────┬───────────┘  │
                    │           │              │
                    │  ┌────────▼──────────┐  │
                    │  │  Admin REST API   │  │  ◄── порт 8081
                    │  └──────────────────┘  │
                    └─────┬──────────┬────────┘
                          │          │
              ┌───────────┘          └───────────┐
              ▼                                   ▼
   ┌─────────────────┐                 ┌─────────────────┐
   │  pg-backend-1   │                 │  pg-backend-2   │
   │  (primary)      │                 │  (replica)      │
   │  порт 5432      │                 │  порт 5432      │
   └─────────────────┘                 └─────────────────┘

              │ метрики
              ▼
   ┌─────────────────┐         ┌──────────────────────┐
   │  Analytics DB   │ ◄─────► │   Web GUI            │
   │  (PostgreSQL)   │         │   (Python + Flask)   │
   │  порт 5432      │         │   порт 8080          │
   └─────────────────┘         └──────────────────────┘
```

- **Клиенты** подключаются к PgPooler по протоколу PostgreSQL (порт 6432).
- **Ядро (C++)** принимает соединения, аутентифицирует, маршрутизирует по пользователю и выдаёт соединение из нужного пула к одному из бэкендов.
- **Бэкенды** — один или несколько экземпляров PostgreSQL (например, primary и replica).
- **Analytics DB** — отдельная PostgreSQL для логов соединений, метрик и событий.
- **Web GUI** — дашборд, список соединений, блокировка пользователей, перезагрузка конфига; общается с ядром через REST API и с аналитикой через БД.

---

## Режимы пулинга

Три режима (как в PgBouncer и Odyssey):

| Режим        | Описание                              | Соединение занято              | Возврат в пул                    |
|-------------|----------------------------------------|--------------------------------|----------------------------------|
| **Session** | Один клиент — одно соединение с PG     | С момента подключения клиента | При отключении клиента           |
| **Transaction** | Один клиент на одну транзакцию   | С BEGIN / первого запроса      | При COMMIT/ROLLBACK (ReadyForQuery='I') |
| **Statement**   | Один запрос — одно соединение     | На время выполнения запроса   | После CommandComplete             |

- **Session** — максимум совместимости, минимум переиспользования.
- **Transaction** — типичный режим для веб-приложений.
- **Statement** — максимальное переиспользование, ограничения по протоколу (например, prepared statements).

Режим задаётся в конфигурации: `defaults.pool_mode`, `backends[].pool_mode`, `routing[].pool_mode` (приоритет: правило → бэкенд). В режимах **transaction** и **statement** соединения с PG возвращаются в пул и переиспользуются для других клиентов с теми же (user, database); при повторном выдаче соединения выполняется `DISCARD ALL`.

---

## Стек технологий

| Компонент        | Технология              | Назначение |
|------------------|-------------------------|------------|
| Ядро прокси      | C++17                   | Производительность, контроль памяти, сокеты |
| Event loop       | libevent                | Неблокирующий I/O, epoll |
| Парсинг XML      | pugixml                 | Конфигурация (header-only) |
| HTTP в ядре      | cpp-httplib             | Admin REST API (встраиваемый сервер) |
| Клиент к PG      | libpq                   | Опционально: метрики, проверки к бэкендам |
| Логирование      | spdlog                  | Быстрые логи |
| Сборка           | CMake                   | Сборка C++ |
| Web GUI          | Python 3 + Flask        | Серверный рендеринг, быстрая разработка |
| Шаблоны GUI      | Jinja2 + HTMX           | Динамика без тяжёлого JS (допустим и JS) |
| Стили GUI        | Pico CSS                | Минималистичный CSS |
| Аналитика        | PostgreSQL 16           | Метрики, логи, события |
| Деплой           | Docker + Docker Compose | Контейнеры: ядро, GUI, бэкенды, analytics |

---

## Компоненты системы

### Ядро (C++)

- **Listener** — приём TCP на порту 6432, парсинг PG Wire Protocol (StartupMessage, Query, ReadyForQuery и т.д.).
- **Authenticator** — проверка пользователя/пароля (например, MD5), при необходимости — через один из бэкендов или локальная таблица.
- **Router** — по имени пользователя (и опционально БД) выбор бэкенда и пула из правил, загруженных из XML.
- **Pool Manager** — пулы соединений к каждому бэкенду; `acquire` / `release` в зависимости от режима (session/transaction/statement).
- **Metrics Collector** — сбор счётчиков и событий, асинхронная отправка в Analytics DB (очередь + отдельный поток или таймер).
- **Admin REST API** — HTTP-сервер на порту 8081: статус, соединения, kill, block/unblock, reload конфига, снимок метрик.

### Модель потоков ядра

- **Main thread** — `libevent` event_base: приём клиентов, события клиентских и бэкендовых сокетов, таймеры (health check, очистка idle).
- **Metrics thread** — чтение из lock-free очереди метрик, batch-запись в Analytics DB раз в N секунд.
- **Admin API thread** — HTTP-сервер cpp-httplib, чтение общего состояния (с мьютексами), выполнение команд (kill, block, reload).

### Web GUI (Python)

- Дашборд: активные соединения, загрузка пулов, статус бэкендов.
- Список соединений с возможностью завершения (kill).
- Пользователи: статистика, блокировка/разблокировка.
- Бэкенды и пулы: состояние, снимки метрик.
- События и, при необходимости, базовая аналитика запросов.
- Обращение к ядру через REST API (например, `api_client.py`), к аналитике — через SQL/ORM.

---

## Конфигурация (XML)

Правила маршрутизации и параметры пулов задаются в XML (файл конфигурации, путь задаётся при запуске):

- Список **бэкендов** (host, port, имя).
- Список **пулов** (имя, бэкенд, режим: session / transaction / statement, размер пула и т.д.).
- **Правила маршрутизации**: пользователь (и при необходимости database) → backend, pool.

Пример идеи (псевдо-XML):

```xml
<backends>
  <backend name="primary" host="pg-backend-1" port="5432"/>
  <backend name="replica" host="pg-backend-2" port="5432"/>
</backends>
<pools>
  <pool name="primary_pool" backend="primary" mode="transaction" size="20"/>
  <pool name="readonly_pool" backend="replica" mode="statement" size="50"/>
</pools>
<routing>
  <route user="writer" pool="primary_pool"/>
  <route user="reader" pool="readonly_pool"/>
</routing>
```

Перезагрузка конфига без остановки сервиса — через `POST /api/config/reload`.

---

## REST API (Admin)

Базовый набор эндпоинтов (порт 8081, JSON):

| Метод | Путь | Описание |
|-------|------|----------|
| GET  | `/api/status` | Общее состояние системы |
| GET  | `/api/connections` | Список активных клиентских сессий |
| POST | `/api/connections/{id}/kill` | Завершить сессию |
| GET  | `/api/pools` | Статистика по пулам |
| GET  | `/api/pools/{name}` | Детали пула |
| GET  | `/api/backends` | Статус бэкендов |
| GET  | `/api/users` | Пользователи и статистика |
| POST | `/api/users/{name}/block` | Заблокировать пользователя |
| POST | `/api/users/{name}/unblock` | Разблокировать пользователя |
| POST | `/api/config/reload` | Перечитать XML-конфиг |
| GET  | `/api/metrics/snapshot` | Текущий снимок метрик |

GUI дергает эти методы для отображения данных и действий (kill, block, reload).

---

## База аналитики

Отдельная PostgreSQL (в Docker — отдельный контейнер) для хранения:

- **connection_log** — история подключений/отключений (client_addr, user, database, backend, pool, время, счётчики запросов/байт, причина отключения).
- **active_connections** — текущие сессии (обновляется ядром или агентом): session_id, user, backend, state (idle/active/waiting), last_activity, current_query.
- **pool_stats_history** — снимки по пулам (active/idle backends, waiting clients, avg_wait_time и т.д.) раз в N секунд.
- **query_stats** — агрегаты по запросам (fingerprint, user, backend, calls, total_time, min/max, last_called).
- **blocked_users** — заблокированные пользователи (username, blocked_at, reason).
- **events** — журнал событий (connect, disconnect, block, kill, error, config_reload).

Индексы по времени и пользователю для быстрых выборок в GUI и отчётах.

---

## Поток данных

Пример: клиент `psql -h localhost -p 6432 -U reader`.

1. Установка TCP-соединения с PgPooler.
2. Приём `StartupMessage` → извлечение `username=reader`.
3. Аутентификация (например, MD5): выдача challenge, проверка ответа.
4. Router: по правилам `reader` → backend=replica, pool=readonly_pool (statement).
5. Отправка клиенту `AuthenticationOk` и `ReadyForQuery`.
6. Клиент шлёт `Query "SELECT * FROM users"`.
7. Pool: `acquire` свободного соединения из `readonly_pool` к pg-backend-2.
8. Проксирование запроса на бэкенд, затем проксирование ответа (RowDescription, DataRow, CommandComplete, ReadyForQuery) клиенту.
9. В режиме statement соединение возвращается в пул (`release`).
10. Метрики: учёт запроса, обновление счётчиков; при необходимости — асинхронная запись в Analytics DB.

Дальнейшие запросы того же клиента снова проходят через acquire → forward → release. Маршрут (replica + readonly_pool) для `reader` не меняется.

---

## Отличия от PgBouncer и Odyssey

| Возможность | PgBouncer | Odyssey | PgPooler |
|-------------|-----------|---------|----------|
| Формат конфигурации | INI | Свой DSL | XML |
| Web GUI | ❌ | ❌ | ✅ |
| Аналитика с историей | ❌ | Частично (логи) | ✅ (PostgreSQL + GUI) |
| Kill / Block через GUI | ❌ | ❌ | ✅ |
| REST API управления | ❌ | ❌ | ✅ |
| Маршрутизация по пользователю | Ограничено | ✅ | ✅ |
| Три режима пулинга | ✅ | ✅ | ✅ |
| Дашборд в реальном времени | ❌ | ❌ | ✅ (HTMX auto-refresh или JS) |

---

## План реализации

| Этап | Содержание | Ориентир |
|------|------------|----------|
| **1. Фундамент** | PG Wire parser, TCP listener (libevent), конечный автомат клиента, прозрачный proxy client ↔ один backend | 2–3 недели |
| **2. Connection Pool** | Pool: acquire/release; режимы session, transaction, statement | 2 недели |
| **3. Routing + Config** | Парсер XML, Router (user → backend + pool), несколько бэкендов и пулов | 1–2 недели |
| **4. Метрики + Analytics DB** | MetricsCollector, запись в PostgreSQL, connection_log, active_connections, pool_stats | 1 неделя |
| **5. Admin REST API** | HTTP-сервер (cpp-httplib), эндпоинты status/connections/kill/block/reload/metrics | 1 неделя |
| **6. Web GUI** | Flask + Jinja2 + HTMX (или JS), дашборд, соединения, пользователи, бэкенды, kill/block | 2 недели |
| **7. Docker Compose** | Dockerfile для ядра, GUI, analytics, несколько PG-бэкендов; README и сценарий демо | 1 неделя |

---

## Как запустить (сейчас: этап 1 — прозрачный proxy)

Нужны: Docker и Docker Compose (Docker Desktop на Windows включает оба). Запустите Docker Desktop перед командами.

**Сборка и запуск:**

```bash
cd PgPooler
docker compose up --build -d
```

Поднимаются три контейнера:
- **postgres** — PostgreSQL 16 (primary) на порту **5432**; базы: `postgres`, `main`, `app`, `default_test`;
- **postgres2** — PostgreSQL 16 (replica) на порту **5433**; базы: `postgres`, `reporting`, `analytics_test`, `dwh_test`; пользователь для тестов: `reporting_reader` / `rreader`;
- **pgpooler** — прокси на порту **6432**.

**Проверка через psql:**

Подключение **напрямую к Postgres** (в обход прокси):

```bash
psql -h localhost -p 5432 -U postgres -d postgres
```

Подключение **через PgPooler** (прокси пересылает всё в Postgres):

```bash
psql -h localhost -p 6432 -U postgres -d postgres
```

Пароль при запросе: `postgres`. После входа можно выполнять любые запросы — они проходят через прокси.

**Подключение из DBeaver (или другого клиента):**

- Хост: `localhost`
- Порт: **6432** (через PgPooler) или **5432** (напрямую к Postgres)
- Пользователь: `postgres`, пароль: `postgres`, база: `postgres`

**Остановка:**

```bash
docker compose down
```

**Тесты маршрутизации:**

Проверка правил из `routing.yaml` (exact/list/prefix/regex/user/default): подключение к pgpooler разными парами user/database и проверка бэкенда по таблице `backend_id`.

```bash
docker compose run --rm test
```

С хоста (если установлен `psql`): `PGHOST=localhost PGPORT=6432 ./tests/run_routing_tests.sh`

**Конфигурация (четыре YAML-файла):**

- **pgpooler.yaml** (основной) — listen, пути к `logging.path`, `backends.path`, `routing.path`. Задаётся через **CONFIG_PATH** (по умолчанию `pgpooler.yaml`).
- **logging.yaml** — уровень, файл (путь или directory+filename в стиле PostgreSQL), append, формат, rotation. Логи только в файл, не в консоль.
- **backends.yaml** — список PostgreSQL-серверов (name, host, port, pool_size).
- **routing.yaml** — дефолты пула и правила маршрутизации (имена backend из backends.yaml).

Подробно: `docs/CONFIG_FORMAT.md`.

Пример `pgpooler.yaml`:

```yaml
listen:
  host: "0.0.0.0"
  port: 6432
logging:
  path: logging.yaml
backends:
  path: backends.yaml
routing:
  path: routing.yaml
```

В образе лежат все четыре конфига. Для Docker смонтируйте `backends.yaml` с `host: postgres` у бэкендов:  
`volumes: - ./backends.yaml:/etc/pgpooler/backends.yaml`

**Переменные окружения:**

| Переменная     | По умолчанию    | Описание |
|----------------|-----------------|----------|
| `CONFIG_PATH`  | `pgpooler.yaml` | Путь к основному конфигу (YAML). |

В дальнейшем через Docker Compose добавятся: Web GUI, analytics-db, маршрутизация по пользователю на разные бэкенды.

---

*Дипломный проект. Тема: программное средство управления пулом соединений и маршрутизации запросов в PostgreSQL.*
