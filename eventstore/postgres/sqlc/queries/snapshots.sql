-- name: InsertSnapshot :one
INSERT INTO "snapshots" (
    "aggregate_type",
    "aggregate_id",
    "version",
    "data",
    "timestamp"
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5
)
RETURNING *;

-- name: GetSnapshot :one
SELECT *
FROM "snapshots"
WHERE "aggregate_id" = $1
ORDER BY "version" DESC
LIMIT 1;