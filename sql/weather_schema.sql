CREATE DATABASE analytics;
\connect analytics;

CREATE TABLE IF NOT EXISTS weather_raw (
    mongo_id TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION trg_weather_raw_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS weather_raw_set_updated_at ON weather_raw;
CREATE TRIGGER weather_raw_set_updated_at
BEFORE UPDATE ON weather_raw
FOR EACH ROW EXECUTE FUNCTION trg_weather_raw_updated_at();

CREATE TABLE IF NOT EXISTS weather_analytics (
    observed_at TIMESTAMPTZ PRIMARY KEY,
    temperature_c NUMERIC,
    humidity INTEGER,
    pressure_mm INTEGER,
    pressure_pa INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION trg_weather_analytics_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS weather_analytics_set_updated_at ON weather_analytics;
CREATE TRIGGER weather_analytics_set_updated_at
BEFORE UPDATE ON weather_analytics
FOR EACH ROW EXECUTE FUNCTION trg_weather_analytics_updated_at();
