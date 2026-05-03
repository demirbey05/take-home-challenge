package producer

// Producer generates tasks with a random type (0-9) and value (0-99),
// saves them to the database with state "received", and pushes them
// to the consumer via REST. It stops when max_backlog is reached.

// TODO: Implement task generation loop, backlog check, REST push
