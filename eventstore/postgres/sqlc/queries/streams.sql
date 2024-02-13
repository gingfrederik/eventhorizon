-- name: UpsertSteam :exec
INSERT INTO "streams" (
    "id",
    "aggregate_type",
    "position",
    "version",
    "updated_at"
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5
) ON CONFLICT (id) 
DO UPDATE SET
    "position" = EXCLUDED.position,
    "version" = EXCLUDED.version,
    "updated_at" = EXCLUDED.updated_at;

-- name: GetStream :one
SELECT * FROM "streams" WHERE "id" = $1;