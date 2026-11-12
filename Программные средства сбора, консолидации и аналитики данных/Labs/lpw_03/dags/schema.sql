CREATE TABLE IF NOT EXISTS stg_happiness (
    country TEXT NOT NULL,
    region TEXT NOT NULL,
    year INTEGER NOT NULL CHECK (year BETWEEN 2015 AND 2019),
    happiness_score DOUBLE PRECISION NOT NULL CHECK (happiness_score BETWEEN 0 AND 10),
    gdp_per_capita DOUBLE PRECISION CHECK (gdp_per_capita >= 0),
    social_support DOUBLE PRECISION CHECK (social_support >= 0),
    region_source TEXT,
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    PRIMARY KEY (country, year)
);
CREATE TABLE IF NOT EXISTS etl_quality_log (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row INTEGER NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('rejected', 'warning')),
    reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS etl_runs (
    run_id TEXT PRIMARY KEY,
    raw_count INTEGER NOT NULL,
    stg_count INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    warning_count INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (raw_count = stg_count + rejected_count)
);
