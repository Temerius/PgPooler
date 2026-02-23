-- PgPooler Analytics DB: initial schema
-- Run against database "pgpooler" on pgpooler-analytics instance.
-- Usage: psql -h pgpooler-analytics -U postgres -d pgpooler -f 001_init_analytics_schema.sql

CREATE SCHEMA IF NOT EXISTS pgpooler;

-- ---------------------------------------------------------------------------
-- 1. History of connections (who, from where, to which backend, mode, duration)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.connection_sessions (
    id                  BIGSERIAL PRIMARY KEY,
    session_id          INT NOT NULL,
    worker_id           SMALLINT,

    client_addr         INET,
    client_port         INT,

    username            TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    backend_name        TEXT NOT NULL,
    pool_mode           TEXT NOT NULL
        CHECK (pool_mode IN ('session', 'transaction', 'statement')),
    application_name    TEXT,

    connected_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    disconnected_at     TIMESTAMPTZ,
    duration_sec        NUMERIC(12, 3),

    disconnect_reason   TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_connection_sessions_connected_at ON pgpooler.connection_sessions (connected_at);
CREATE INDEX idx_connection_sessions_username ON pgpooler.connection_sessions (username);
CREATE INDEX idx_connection_sessions_backend ON pgpooler.connection_sessions (backend_name);
CREATE INDEX idx_connection_sessions_disconnected_at ON pgpooler.connection_sessions (disconnected_at) WHERE disconnected_at IS NOT NULL;
CREATE INDEX idx_connection_sessions_active ON pgpooler.connection_sessions (connected_at) WHERE disconnected_at IS NULL;

-- ---------------------------------------------------------------------------
-- 2. Query fingerprints (normalized query text for aggregation)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.query_fingerprints (
    id                  BIGSERIAL PRIMARY KEY,
    fingerprint         TEXT NOT NULL,
    fingerprint_hash    BYTEA,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX idx_query_fingerprints_hash ON pgpooler.query_fingerprints (fingerprint_hash) WHERE fingerprint_hash IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. Individual queries (each request passing through pooler)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.queries (
    id                      BIGSERIAL PRIMARY KEY,
    connection_session_id   BIGINT,
    session_id              INT NOT NULL,
    fingerprint_id          BIGINT REFERENCES pgpooler.query_fingerprints(id),

    username                TEXT NOT NULL,
    database_name           TEXT NOT NULL,
    backend_name            TEXT NOT NULL,
    application_name        TEXT,

    query_text              TEXT,
    query_text_length       INT,

    started_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at             TIMESTAMPTZ,
    duration_ms             NUMERIC(12, 2),

    command_type            TEXT,
    rows_affected           BIGINT,
    rows_returned           BIGINT,

    bytes_to_backend        BIGINT DEFAULT 0,
    bytes_from_backend      BIGINT DEFAULT 0,

    error_sqlstate         CHAR(5),
    error_message          TEXT,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_queries_started_at ON pgpooler.queries (started_at);
CREATE INDEX idx_queries_username ON pgpooler.queries (username);
CREATE INDEX idx_queries_backend ON pgpooler.queries (backend_name);
CREATE INDEX idx_queries_fingerprint ON pgpooler.queries (fingerprint_id);
CREATE INDEX idx_queries_duration ON pgpooler.queries (duration_ms) WHERE duration_ms IS NOT NULL;
CREATE INDEX idx_queries_connection_session ON pgpooler.queries (connection_session_id) WHERE connection_session_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 4. Pool snapshots (periodic: idle/active/waiting per backend+user+db)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.pool_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    snapshot_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),

    backend_name        TEXT NOT NULL,
    username            TEXT NOT NULL,
    database_name       TEXT NOT NULL,
    pool_mode           TEXT NOT NULL,

    pool_size           INT NOT NULL,
    idle_count          INT NOT NULL DEFAULT 0,
    active_count        INT NOT NULL DEFAULT 0,
    waiting_count       INT NOT NULL DEFAULT 0,
    avg_wait_ms         NUMERIC(12, 2)
);

CREATE INDEX idx_pool_snapshots_snapshot_at ON pgpooler.pool_snapshots (snapshot_at);
CREATE INDEX idx_pool_snapshots_backend ON pgpooler.pool_snapshots (backend_name, snapshot_at);

-- ---------------------------------------------------------------------------
-- 5. Event log (audit: connect, disconnect, kill, block, reload, error)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.events (
    id                  BIGSERIAL PRIMARY KEY,
    event_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    event_type          TEXT NOT NULL,
    severity            TEXT,

    username            TEXT,
    database_name       TEXT,
    backend_name        TEXT,
    session_id          INT,
    client_addr         INET,

    message             TEXT,
    details             JSONB,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_events_event_at ON pgpooler.events (event_at);
CREATE INDEX idx_events_event_type ON pgpooler.events (event_type);
CREATE INDEX idx_events_username ON pgpooler.events (username);
CREATE INDEX idx_events_details_gin ON pgpooler.events USING GIN (details);

-- ---------------------------------------------------------------------------
-- 6. Blocked users (current state; history can be in events)
-- ---------------------------------------------------------------------------
CREATE TABLE pgpooler.blocked_users (
    id                  SERIAL PRIMARY KEY,
    username            TEXT NOT NULL UNIQUE,
    blocked_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    reason              TEXT,
    blocked_by          TEXT,
    unblocked_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX idx_blocked_users_username ON pgpooler.blocked_users (username);
CREATE INDEX idx_blocked_users_unblocked ON pgpooler.blocked_users (unblocked_at) WHERE unblocked_at IS NULL;

-- Grants for analytics writer (user pgpooler)
GRANT USAGE ON SCHEMA pgpooler TO pgpooler;
GRANT INSERT, UPDATE, SELECT ON pgpooler.connection_sessions TO pgpooler;
GRANT INSERT, UPDATE, SELECT ON pgpooler.queries TO pgpooler;
GRANT INSERT, SELECT ON pgpooler.query_fingerprints TO pgpooler;
GRANT INSERT, SELECT ON pgpooler.pool_snapshots TO pgpooler;
GRANT INSERT, SELECT ON pgpooler.events TO pgpooler;
GRANT INSERT, UPDATE, SELECT ON pgpooler.blocked_users TO pgpooler;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA pgpooler TO pgpooler;

-- Active sessions: use connection_sessions WHERE disconnected_at IS NULL (no separate table)
