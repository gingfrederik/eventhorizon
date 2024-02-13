CREATE TABLE IF NOT EXISTS "streams" (
    "id" UUID PRIMARY KEY,
    "aggregate_type" VARCHAR(2048) NOT NULL,
    "position" BIGINT NOT NULL,
    "version" INTEGER NOT NULL,
    "updated_at" TIMESTAMP NOT NULL
);
