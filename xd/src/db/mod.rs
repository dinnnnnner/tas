pub const SCHEMA_SQL: &str = r#"
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'telemetry_samples'
    ) AND NOT EXISTS (
        SELECT 1
        FROM pg_partitioned_table pt
        JOIN pg_class c ON c.oid = pt.partrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname = 'telemetry_samples'
    ) THEN
        EXECUTE 'ALTER TABLE telemetry_samples RENAME TO telemetry_samples_legacy';
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS telemetry_samples (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id TEXT NOT NULL,
    sensor_id INTEGER NOT NULL,
    axis TEXT NOT NULL DEFAULT '',
    alarm_bit BOOLEAN NOT NULL DEFAULT FALSE,
    t_sec DOUBLE PRECISION NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    request_id BIGINT NOT NULL,
    PRIMARY KEY (created_at, id)
) PARTITION BY RANGE (created_at);

CREATE TABLE IF NOT EXISTS alarm_events (
    id BIGSERIAL PRIMARY KEY,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    device_id TEXT NOT NULL,
    alarm_id TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    cleared BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS system_events (
    id BIGSERIAL PRIMARY KEY,
    ts_ms BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level TEXT NOT NULL,
    message TEXT NOT NULL
);

ALTER TABLE alarm_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE alarm_events ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;
UPDATE alarm_events
SET created_at = to_timestamp(ts_ms::double precision / 1000.0)
WHERE created_at IS NULL;
ALTER TABLE alarm_events ALTER COLUMN created_at SET NOT NULL;

ALTER TABLE system_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
ALTER TABLE system_events ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP;
UPDATE system_events
SET created_at = to_timestamp(ts_ms::double precision / 1000.0)
WHERE created_at IS NULL;
ALTER TABLE system_events ALTER COLUMN created_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alarm_ts_ms ON alarm_events (ts_ms);
CREATE INDEX IF NOT EXISTS idx_alarm_created_at ON alarm_events (created_at);
CREATE INDEX IF NOT EXISTS idx_alarm_device_alarmid_ts
    ON alarm_events (device_id, alarm_id, ts_ms);

CREATE INDEX IF NOT EXISTS idx_system_events_ts_ms ON system_events (ts_ms);
CREATE INDEX IF NOT EXISTS idx_system_events_created_at ON system_events (created_at);

CREATE OR REPLACE FUNCTION ensure_telemetry_partition_for_day(target_day DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    partition_name TEXT := format('telemetry_samples_%s', to_char(target_day, 'YYYY_MM_DD'));
    start_ts TIMESTAMPTZ := target_day::timestamptz;
    end_ts TIMESTAMPTZ := (target_day + 1)::timestamptz;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF telemetry_samples
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_ts,
        end_ts
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (ts_ms)',
        partition_name || '_ts_ms_idx',
        partition_name
    );
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (device_id, sensor_id, ts_ms)',
        partition_name || '_device_sensor_ts_idx',
        partition_name
    );
    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS %I ON %I (device_id, axis, ts_ms)',
        partition_name || '_device_axis_ts_idx',
        partition_name
    );
END
$$;

SELECT ensure_telemetry_partition_for_day(CURRENT_DATE);
SELECT ensure_telemetry_partition_for_day(CURRENT_DATE + 1);
"#;
