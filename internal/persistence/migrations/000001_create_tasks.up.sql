CREATE TABLE IF NOT EXISTS tasks (
    id         BIGSERIAL PRIMARY KEY,
    type       INTEGER NOT NULL CHECK (type >= 0 AND type <= 9),
    value      INTEGER NOT NULL CHECK (value >= 0 AND value <= 99),
    state      TEXT    NOT NULL DEFAULT 'received' CHECK (state IN ('received', 'processing', 'done')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_state ON tasks(state);
