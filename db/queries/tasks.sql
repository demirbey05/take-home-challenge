-- name: CreateTask :one
INSERT INTO tasks (type, value, state)
VALUES ($1, $2, 'received')
RETURNING id, type, value, state, created_at, updated_at;

-- name: GetTask :one
SELECT id, type, value, state, created_at, updated_at
FROM tasks
WHERE id = $1;

-- name: UpdateTaskState :exec
UPDATE tasks
SET state = $2, updated_at = NOW()
WHERE id = $1;

-- name: CountTasksByState :one
SELECT COUNT(*) FROM tasks WHERE state = $1;

-- name: ListPendingTasks :many
SELECT id, type, value, state, created_at, updated_at
FROM tasks
WHERE state = 'received'
ORDER BY created_at ASC
LIMIT $1;

-- name: SumProcessedValues :one
SELECT COALESCE(SUM(value), 0)::bigint FROM tasks WHERE state = 'done';
