CREATE TABLE IF NOT EXISTS daily_stats (
    date              DATE        NOT NULL,
    country           TEXT        NOT NULL,
    confirmed         BIGINT      NOT NULL,
    recovered         BIGINT      NOT NULL,
    deaths            BIGINT      NOT NULL,
    new_confirmed     BIGINT      NOT NULL,
    new_deaths        BIGINT      NOT NULL,
    new_confirmed_7d_avg DOUBLE PRECISION,
    case_fatality_pct DOUBLE PRECISION,
    is_correction     BOOLEAN     NOT NULL,
    PRIMARY KEY (date, country)
);
CREATE INDEX IF NOT EXISTS idx_daily_country ON daily_stats (country, date);

CREATE TABLE IF NOT EXISTS monthly_summary (
    country        TEXT   NOT NULL,
    year           INT    NOT NULL,
    month          INT    NOT NULL,
    new_confirmed  BIGINT NOT NULL,
    new_deaths     BIGINT NOT NULL,
    confirmed_eom  BIGINT NOT NULL,
    PRIMARY KEY (country, year, month)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        SERIAL PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL,
    rows_in       BIGINT,
    rows_out      BIGINT,
    quality_warnings INT,
    seconds_total DOUBLE PRECISION,
    metrics_json  JSONB
);
