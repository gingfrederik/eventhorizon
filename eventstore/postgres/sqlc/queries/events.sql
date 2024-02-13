-- name: InsertEvents :batchmany
INSERT INTO "events" (
    "event_type",
    "timestamp",
    "aggregate_type",
    "aggregate_id",
    "version",
    "data",
    "metadata"
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
) RETURNING "position";

-- name: ListEventsByAggregateID :many
SELECT *
FROM "events"
WHERE "aggregate_id" = $1
ORDER BY "version" ASC;

-- name: ListEventsByAggregateIDAndVersion :many
SELECT *
FROM "events"
WHERE "aggregate_id" = $1
AND "version" >= $2
ORDER BY "version" ASC;