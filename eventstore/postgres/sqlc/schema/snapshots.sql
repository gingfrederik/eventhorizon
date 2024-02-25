CREATE TABLE IF NOT EXISTS "snapshots" (
    "aggregate_id" UUID PRIMARY KEY,
    "aggregate_type" VARCHAR(2048) NOT NULL,
    "version" INTEGER NOT NULL,
    "data" BYTEA NOT NULL,
    "timestamp" TIMESTAMP NOT NULL
);