# Формат конфигурации: маршрутизация и пулы

Главная фишка PgPooler — **понятные правила «кто куда и в каком режиме»**. Конфигурация разделена на **два файла**.

---

## 0. Четыре конфига (YAML)

| Конфиг | Файл | Содержимое |
|--------|------|------------|
| **Основной** | **pgpooler.yaml** | listen, пути к logging/backends/routing (`logging.path`, `backends.path`, `routing.path`). |
| **Логирование** | **logging.yaml** | Уровень, файл (путь или directory+filename с strftime), append, формат, rotation (как в PostgreSQL). В консоль не пишем. |
| **Бэкенды** | **backends.yaml** | Список PostgreSQL-серверов (name, host, port, pool_size). Имена используются в routing. |
| **Маршрутизация** | **routing.yaml** | Дефолты пула и правила маршрутизации (без бэкендов — имена backend из backends.yaml). |

**CONFIG_PATH** — основной конфиг (по умолчанию `pgpooler.yaml`). Все пути в нём могут быть относительными (от каталога основного конфига).

**Пример pgpooler.yaml:**

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

**Пример logging.yaml (в стиле PostgreSQL):**

```yaml
level: info
destination: file
file:
  path: /var/log/pgpooler/pgpooler.log
  append: true
  rotation_age: 86400       # секунд (1 день) — новый файл по возрасту
  rotation_size_mb: 100     # новый файл при достижении размера (MB)
format: text
```

Можно задать `file.directory` и `file.filename` (шаблон strftime, напр. `pgpooler-%Y-%m-%d.log`) вместо `file.path`.

---

## 1. Структура routing.yaml и backends.yaml

**backends.yaml** — только список бэкендов. **routing.yaml** — только defaults и правила (поле `backend` в правилах — имя из backends.yaml).

```yaml
# backends.yaml
backends:
  - name: primary
    host: pg-primary
    port: 5432
    pool_size: 10
  - name: replica
    host: pg-replica
    port: 5432

# routing.yaml
defaults:
  pool_size: 20
routing:
  - database: reporting
    backend: replica
  - database: [main, app, postgres]
    backend: primary
  - default: true
    backend: primary
```

Идея: **backends** задают «куда можно ходить»; **routing** — «кто куда идёт» по правилам (database/user → backend name).

---

## 2. Сопоставление (match): как задаём «кто»

Один маршрут — это «по каким полям отбираем» + «куда и в каком режиме».

### 2.1. Поле `database`

Поддерживаем несколько способов задать значение:

| Формат | Пример | Значение |
|--------|--------|----------|
| Один литерал | `database: postgres` | Точное совпадение с `postgres`. |
| Список | `database: [main, app, postgres]` | Любая из перечисленных баз. |
| Префикс (glob) | `database: "app_*"` | База начинается с `app_` (звёздочка — «что угодно»). |
| Регулярное выражение | `database: ~ "^(reporting\|analytics)_"` | Совпадение с regex (синтаксис движка в C++). |

Удобно договориться:
- `database: "name"` — всегда точное.
- `database: "prefix*"` — один суффикс `*` в конце = префикс.
- `database: ~ "regex"` — явно регулярка.
- `database: [a, b, c]` — список (без кавычек можно, если значения без пробелов).

Так админ видит сразу: точное, список, префикс или regex.

### 2.2. Поле `user` (опционально)

Те же варианты, что и для `database`:

```yaml
- user: reporting_readonly
  backend: replica

- user: [app_user, web_user]
  backend: primary

- user: "reader_*"
  backend: replica

- user: ~ "^reporting_"
  backend: replica
```

Правило может содержать и `database`, и `user` — тогда логика **AND** (должны совпасть оба). Пример: «база = reporting и user = reader_*» → replica.

### 2.3. Правило по умолчанию

Одно из правил должно ловить «всё остальное»:

```yaml
- default: true
  backend: primary
```

Обычно его ставят **последним**. Если ни одно правило не сработало — можно либо считать ошибкой (нет маршрута), либо неявный default на первый бэкенд; лучше задать явно.

---

## 3. Режим пула (pool_mode)

Три режима (как в README):

| Режим | Когда соединение с PG занято | Когда возвращается в пул |
|-------|-----------------------------|---------------------------|
| **session** | Вся сессия клиента | При отключении клиента |
| **transaction** | Одна транзакция (BEGIN..COMMIT/ROLLBACK) | После COMMIT/ROLLBACK |
| **statement** | Один запрос | После CommandComplete |

Задаётся в трёх местах (приоритет — от более конкретного к общему):

1. **В правиле маршрута** — для этого маршрута:  
   `routing[].pool_mode: transaction`
2. **У бэкенда** — для всех пулов к этому бэкенду, если в правиле не указано:  
   `backends[].pool_mode`
3. **В defaults** — глобально:  
   `defaults.pool_mode`

Пример:

```yaml
defaults:
  pool_mode: session

backends:
  - name: primary
    host: pg-primary
    port: 5432
    pool_mode: transaction
    pool_size: 30

  - name: replica
    host: pg-replica
    port: 5432
    pool_size: 50

routing:
  - database: reporting
    backend: replica
    pool_mode: statement
  - default: true
    backend: primary
```

Для `reporting` → replica в режиме **statement**; для всего остального → primary в режиме **transaction** (из бэкенда).

---

## 4. Размер пула (pool_size)

- **defaults.pool_size** — по умолчанию для всех пулов.
- **backends[].pool_size** — для пула к этому бэкенду.
- **routing[].pool_size** — только для соединений, попавших в это правило (опционально; если не задано — из бэкенда или defaults).

Так можно ограничить нагрузку на реплику (например 50) и по-другому — на primary (20).

---

## 5. Таймаут простоя сессии (session_idle_timeout)

Как в DBeaver: если клиент **ничего не шлёт** (ни запросов, ни keepalive) в течение заданного времени — сессия закрывается. Удобно, чтобы «забытые» подключения не висели часами.

| Параметр | Значение | Смысл |
|----------|----------|--------|
| **session_idle_timeout** | число (секунды) | Закрыть клиентское соединение после стольких секунд **простоя** (нет данных от клиента). |
| | **0** или не задано | Таймаут не применять, сессия может висеть бесконечно. |

Задаётся на **трёх уровнях** (приоритет: правило → бэкенд → defaults):

1. **defaults.session_idle_timeout** — глобально для всех сессий.
2. **backends[].session_idle_timeout** — для всех сессий к этому бэкенду (если в правиле не переопределено).
3. **routing[].session_idle_timeout** — только для соединений, попавших в это правило.

Пример: по умолчанию 1 час, для reporting — 2 часа:

```yaml
defaults:
  pool_mode: session
  pool_size: 20
  session_idle_timeout: 3600    # 1 час

backends:
  - name: primary
    host: pg-primary
    port: 5432
    session_idle_timeout: 1800 # 30 мин для primary

  - name: replica
    host: pg-replica
    port: 5432

routing:
  - database: reporting
    backend: replica
    session_idle_timeout: 7200  # 2 часа для отчётов
  - default: true
    backend: primary
```

Реализация: в ядре по каждому клиентскому соединению заводим таймер «последняя активность»; при любых данных от клиента — сбрасываем. Периодический таск (или event с таймаутом) проверяет: если `now - last_activity > session_idle_timeout` — закрываем сессию (и при session-режиме возвращаем серверное соединение в пул / закрываем его).

---

## 6. Полный пример «на вырост»

```yaml
listen:
  host: "0.0.0.0"
  port: 6432

log:
  level: info

defaults:
  pool_mode: session
  pool_size: 20
  session_idle_timeout: 3600   # 1 час простоя — закрыть (0 = выключено)

backends:
  - name: primary
    host: pg-primary
    port: 5432
    pool_mode: transaction
    pool_size: 25
    session_idle_timeout: 1800

  - name: replica
    host: pg-replica
    port: 5432
    pool_mode: statement
    pool_size: 50

routing:
  # Точная база → реплика, режим statement, свой таймаут простоя
  - database: reporting
    backend: replica
    pool_mode: statement
    session_idle_timeout: 7200

  # Список баз → primary
  - database: [main, app, postgres]
    backend: primary

  # Префикс: всё что начинается с analytics_
  - database: "analytics_*"
    backend: replica

  # Регулярка: reporting_ или dwh_
  - database: ~ "^(reporting|dwh)_"
    backend: replica

  # По пользователю (если нужен только user без database)
  - user: readonly_user
    backend: replica

  # Комбо: и база, и пользователь
  - database: reporting
    user: ~ "^reporting_"
    backend: replica
    pool_mode: transaction

  # Всё остальное → primary
  - default: true
    backend: primary
```

Подключаешься к любой базе/пользователю — пулер сам выбирает бэкенд и режим пула по первому совпавшему правилу.

---

## 7. Синтаксис в одном месте (шпаргалка для админа)

| Что | Как писать | Пример |
|-----|------------|--------|
| Точное значение | `field: value` | `database: postgres` |
| Список | `field: [a, b, c]` | `database: [main, app]` |
| Префикс (glob) | `field: "prefix*"` | `database: "app_*"` |
| Регулярное выражение | `field: ~ "regex"` | `database: ~ "^reporting_"` |
| Правило по умолчанию | `default: true` | в конце списка `routing` |
| Режим пула | `pool_mode: session \| transaction \| statement` | в правиле или у бэкенда |
| Размер пула | `pool_size: N` | в defaults, у бэкенда или в правиле |
| Таймаут простоя сессии | `session_idle_timeout: N` (сек) | в defaults, у бэкенда или в правиле; 0 = выключено |

---

## 8. Реализация (по шагам)

1. **Парсинг YAML** в C++ (yaml-cpp или свой минимальный парсер для нашей схемы).
2. **Модель в памяти**: backends, defaults, список правил; у каждого правила — типы матчеров (exact / list / prefix / regex) для `database` и при необходимости `user`.
3. **При подключении клиента**: берём `user` и `database` из StartupMessage, проходим по правилам по порядку; первое совпадение → backend + pool_mode + pool_size + session_idle_timeout (каждое — из правила, иначе из бэкенда, иначе из defaults).
4. **Совпадение**: exact — сравнение строки; list — вхождение в множество; prefix — проверка `starts_with` и один `*` в конце; regex — вызов движка (например `std::regex`).
5. **Таймаут простоя**: для каждой клиентской сессии храним время последней активности (данные от клиента); периодически или по таймеру проверяем: если прошло больше `session_idle_timeout` секунд — закрываем сессию.

Такой формат даёт «очень удобный и понятный» конфиг: и регулярные выражения, и списки, и префиксы, и явный default, и явные режимы пула — всё в одном месте, в читаемом виде. Если захочешь, можно в следующем шаге сузить формат (например убрать `user` из первой версии или оставить только database) или добавить ещё поля (например `application_name` потом) без смены общей схемы.
