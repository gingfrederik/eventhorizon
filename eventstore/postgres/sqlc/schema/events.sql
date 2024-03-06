CREATE TABLE IF NOT EXISTS "events" (
    "position" BIGSERIAL PRIMARY KEY,
    "event_type" VARCHAR(2048) NOT NULL,
    "timestamp" TIMESTAMP NOT NULL,
    "aggregate_type" VARCHAR(2048) NOT NULL,
    "aggregate_id" UUID NOT NULL,
    "version" INTEGER NOT NULL,
    "data" JSONB,
    "metadata" JSONB
);

CREATE INDEX IF NOT EXISTS "events_aggregate_id_version_idx" ON "events" ("aggregate_id", "version");