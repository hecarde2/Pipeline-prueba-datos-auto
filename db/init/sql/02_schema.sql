-- ===================== ARQUITECTURA MEDALLÓN: bronze -> silver -> gold =====================
REVOKE ALL ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA bronze; CREATE SCHEMA silver; CREATE SCHEMA gold;

-- ---------- BRONZE: dato tal como llega + auditoría de completitud ----------
CREATE TABLE bronze.usgs_raw (
  id             BIGSERIAL PRIMARY KEY,
  source         TEXT NOT NULL CHECK (source IN ('feed_hour','query_window')),
  window_start   TIMESTAMPTZ,
  window_end     TIMESTAMPTZ,
  fetched_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  expected_count INT NOT NULL,            -- lo que la API dice que hay
  received_count INT NOT NULL,            -- lo que realmente llegó
  sha256         TEXT NOT NULL,
  payload        JSONB NOT NULL,
  status         TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processed','failed')),
  error          TEXT,
  processed_at   TIMESTAMPTZ,
  CONSTRAINT complete_batch CHECK (expected_count = received_count)  -- red de seguridad en BD
);
-- Idempotencia: el mismo payload para la misma ventana nunca se inserta dos veces
CREATE UNIQUE INDEX uq_bronze_batch ON bronze.usgs_raw
  (source, COALESCE(window_start,'epoch'::timestamptz), COALESCE(window_end,'epoch'::timestamptz), sha256);
CREATE INDEX ix_bronze_pending ON bronze.usgs_raw (status) WHERE status = 'pending';

-- ---------- SILVER: limpio y tipado ----------
CREATE TABLE silver.events (
  event_id     TEXT PRIMARY KEY,
  event_time   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  magnitude    NUMERIC(4,2),
  mag_type     TEXT,
  place        TEXT,
  longitude    DOUBLE PRECISION NOT NULL,
  latitude     DOUBLE PRECISION NOT NULL,
  depth_km     DOUBLE PRECISION,
  tsunami      SMALLINT,
  significance INT,
  status       TEXT,
  event_type   TEXT,
  url          TEXT,
  bronze_id    BIGINT NOT NULL,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_silver_bronze ON silver.events (bronze_id);

-- ---------- GOLD: listo para consumo (+ métricas de calidad e IA) ----------
CREATE TABLE gold.events (
  event_id     TEXT PRIMARY KEY,
  event_time   TIMESTAMPTZ NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL,
  magnitude    NUMERIC(4,2),
  mag_bucket   TEXT NOT NULL,
  mag_type     TEXT,
  place        TEXT,
  region       TEXT NOT NULL,
  longitude    DOUBLE PRECISION NOT NULL,
  latitude     DOUBLE PRECISION NOT NULL,
  depth_km     DOUBLE PRECISION,
  tsunami      SMALLINT,
  significance INT,
  status       TEXT,
  url          TEXT,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_gold_time ON gold.events (event_time DESC);
CREATE INDEX ix_gold_loaded ON gold.events (loaded_at);
CREATE INDEX ix_gold_region ON gold.events (region);

CREATE TABLE gold.hourly_stats (
  hour          TIMESTAMPTZ PRIMARY KEY,
  events        INT NOT NULL,
  max_magnitude NUMERIC(4,2),
  avg_magnitude NUMERIC(4,2)
);

CREATE TABLE gold.quality_runs (
  id              BIGSERIAL PRIMARY KEY,
  run_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
  dag_id          TEXT NOT NULL,
  run_id          TEXT,
  bronze_id       BIGINT,
  rules_evaluated INT NOT NULL,
  rules_passed    INT NOT NULL,
  rules_failed    INT NOT NULL,
  score           NUMERIC(5,2) NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('passed','failed')),
  checks          JSONB NOT NULL
);

CREATE TABLE gold.daily_reports (
  report_date    DATE PRIMARY KEY,
  headline       TEXT NOT NULL,
  summary        TEXT NOT NULL,
  highlights     JSONB NOT NULL,
  facts          JSONB NOT NULL,         -- cifras calculadas por código
  generated_by   TEXT NOT NULL CHECK (generated_by IN ('llm','fallback')),
  model          TEXT,
  prompt_version TEXT,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE gold.anomalies (
  hour        TIMESTAMPTZ PRIMARY KEY,
  events      INT NOT NULL,
  baseline    NUMERIC(10,2) NOT NULL,
  zscore      NUMERIC(8,2) NOT NULL,
  explanation TEXT NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE gold.ai_runs (
  id             BIGSERIAL PRIMARY KEY,
  run_ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  task           TEXT NOT NULL,
  model          TEXT,
  prompt_version TEXT,
  tokens_in      INT, tokens_out INT, latency_ms INT,
  cost_usd       NUMERIC(10,6) NOT NULL DEFAULT 0,   -- modelo local = 0
  status         TEXT NOT NULL,
  error          TEXT
);

-- ===================== MÍNIMO PRIVILEGIO =====================
-- ingest_user: solo escribe/lee bronze
GRANT USAGE ON SCHEMA bronze TO ingest_user;
GRANT SELECT, INSERT ON bronze.usgs_raw TO ingest_user;
GRANT USAGE ON SEQUENCE bronze.usgs_raw_id_seq TO ingest_user;

-- transform_user: lee bronze (y marca estado), escribe silver y gold
GRANT USAGE ON SCHEMA bronze, silver, gold TO transform_user;
GRANT SELECT ON bronze.usgs_raw TO transform_user;
GRANT UPDATE (status, error, processed_at) ON bronze.usgs_raw TO transform_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA silver, gold TO transform_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA silver, gold TO transform_user;

-- reader_user (API + agente IA): SOLO lectura de gold
GRANT USAGE ON SCHEMA gold TO reader_user;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO reader_user;
ALTER ROLE reader_user SET default_transaction_read_only = on;
ALTER ROLE reader_user SET statement_timeout = '8s';
