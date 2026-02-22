# Единая sequence-диаграмма: подключение клиента и работа пула (все режимы)

Одна диаграмма охватывает общий путь до конца auth и три режима пула (session / transaction / statement): когда соединение уходит в пул и когда берётся из пула. Для каждой клиентской сессии действует **один** режим (задаётся роутингом по user/database); на диаграмме показаны обе ветки после auth (Session и Transaction/Statement) как альтернативы.

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant PgPooler as PgPooler (Dispatcher + Worker)
    participant Session as ClientSession
    participant Pool as Пул (backend, user, db)
    participant PG as PostgreSQL

    %% ========== ОБЩАЯ ЧАСТЬ: до ReadyForQuery ==========
    Client->>PgPooler: TCP connect
    PgPooler->>PgPooler: SSL N (или negotiation)
    Client->>PgPooler: Startup (user, database)
    PgPooler->>PgPooler: routing → backend, pool_mode
    PgPooler->>Session: handoff fd + payload → Worker
    Session->>Session: state = ReadingFirst → received startup
    Session->>PG: connect_to_backend(): новый TCP
    PG-->>Session: connected
    Session->>Session: state = ConnectingToBackend → CollectingStartupResponse
    Session->>PG: Startup (клиентский)
    PG->>Session: Auth (R/p), ParameterStatus (S), BackendKeyData (K), ReadyForQuery (Z)
    Session->>Client: проксируем все сообщения до Z
    Session->>Session: ReadyForQuery получен → конец auth

    %% ========== ВЕТВЛЕНИЕ ПО РЕЖИМУ ==========
    rect rgb(240, 248, 255)
        Note over Session, Pool: Session mode
        Session->>Pool: take(backend, user, db)
        alt в пуле есть idle
            Pool-->>Session: IdleConnection
            Session->>Session: close_auth_backend() — закрыть auth-соединение
            Session->>Session: использовать соединение из пула, state = Forwarding
            Note right of Session: "auth done, handing off to pooled connection"
        else пул пустой
            Session->>Session: оставить текущее auth-соединение, state = Forwarding
            Note right of Session: "auth done, keeping auth connection (pool had no idle)"
        end
        Session->>Client: дальше все запросы по этому соединению
        Session->>PG: запросы/ответы (Forwarding)
        Note over Session, PG: до отключения клиента соединение не возвращается в пул
        Client->>Session: disconnect
        Session->>Pool: put(bev, cached_startup) — вернуть в пул
        Session->>Session: destroy
    end

    rect rgb(255, 248, 240)
        Note over Session, Pool: Transaction / Statement mode (после auth)
        Session->>Pool: put(auth bev, cached_startup) — auth-соединение в пул
        Session->>Session: close_auth_backend(), state = WaitingForBackend
        Note right of Session: "auth done, returning auth connection to pool, will take on first query"
    end

    %% ========== ПОВТОРНЫЙ ВЗЯТИЕ ИЗ ПУЛА (Transaction/Statement) ==========
    Client->>Session: первый запрос (или следующий после return)
    Session->>Pool: take(backend, user, db)
    alt есть в пуле
        Pool-->>Session: IdleConnection (bev + cached_startup)
        Session->>PG: привязка bev к сессии
        Session->>PG: DISCARD ALL (очистить сессию перед использованием)
        Session->>Session: state = SendingDiscardAll
        PG->>Session: ReadyForQuery (Z)
        Session->>Session: state = Forwarding
        Note right of Session: "reusing pooled connection"
        Session->>PG: запрос клиента
        PG->>Session: ответы… ReadyForQuery (Z)
    else пул пустой — новое соединение
        Session->>PG: новый TCP, Startup, auth… ReadyForQuery
        Session->>Session: state = Forwarding
        Session->>PG: запросы клиента
        PG->>Session: ответы… ReadyForQuery (Z)
    end

    %% ========== ВОЗВРАТ В ПУЛ (Transaction vs Statement) ==========
    Note over Session, PG: Transaction: возврат только при ReadyForQuery с tx_state = 'I' (idle)
    Note over Session, PG: Statement: возврат при каждом ReadyForQuery (Z)
    Session->>Session: после подходящего ReadyForQuery
    Session->>Pool: put(bev, cached_startup)
    Session->>Session: state = WaitingForBackend, backend отвязан
    Note right of Session: "returning connection to pool (mode=transaction)" / statement
    Client->>Session: следующий запрос
    Session->>Pool: take(...) — цикл повторяется
```

## Кратко по режимам

| Режим       | После auth (ReadyForQuery)                    | Когда вернуть в пул                          | Когда взять из пула снова      |
|------------|----------------------------------------------|---------------------------------------------|--------------------------------|
| **Session**   | Взять из пула (если есть) или оставить auth | Только при отключении клиента               | Не нужно — одно соединение на сессию |
| **Transaction** | Auth-соединение в пул, клиент без backend   | После каждого COMMIT/ROLLBACK (ReadyForQuery 'I') | На каждый следующий запрос/транзакцию |
| **Statement**  | То же, что Transaction                       | После каждого ReadyForQuery (каждый запрос)  | На каждый следующий запрос    |

## Состояния ClientSession (для диаграммы)

- **ReadingFirst** — ждём первый пакет (Startup) от клиента.
- **ConnectingToBackend** — подключаемся к PostgreSQL.
- **CollectingStartupResponse** — новое соединение: кэшируем ответ до ReadyForQuery.
- **SendingDiscardAll** — взяли из пула: отправили DISCARD ALL, ждём ReadyForQuery.
- **Forwarding** — проксируем трафик клиент ↔ backend.
- **WaitingForBackend** — (только transaction/statement) auth пройден, соединение в пуле, ждём следующий запрос от клиента, чтобы снова взять из пула.

## Пул

- Ключ: `(backend_name, user, database)`.
- **take** — забрать idle-соединение (если есть и не истекло по idle/lifetime).
- **put** — вернуть соединение в пул (bev отвязывается от сессии, кэш startup сохраняется).
