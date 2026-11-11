CREATE TABLE IF NOT EXISTS src.dataset_files (
    dataset_name VARCHAR(80) PRIMARY KEY,
    source_file TEXT NOT NULL,
    encoding VARCHAR(20) NOT NULL,
    delimiter CHAR(1) NOT NULL,
    header_lines JSONB NOT NULL,
    data_start_row INTEGER NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 0,
    sha256 CHAR(64) NOT NULL,
    temporal_type VARCHAR(20) NOT NULL CHECK (temporal_type IN ('monthly', 'quarterly', 'annual', 'static')),
    time_field TEXT,
    series_keys JSONB NOT NULL DEFAULT '[]'::jsonb,
    value_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.load_log (
    load_id UUID PRIMARY KEY,
    dataset_name VARCHAR(80) NOT NULL,
    source_file TEXT NOT NULL,
    file_hash CHAR(64) NOT NULL,
    rows_loaded INTEGER NOT NULL,
    run_type VARCHAR(30) NOT NULL CHECK (run_type IN ('raw_load', 'synthetic_projection')),
    model_name VARCHAR(40),
    synthetic_run_id UUID,
    horizon INTEGER,
    status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_message TEXT
);

DO $$
DECLARE
    dataset_name text;
    datasets text[] := ARRAY[
        'cost_of_living',
        'foreign_affiliates_gender',
        'eurostat_hitech_employment',
        'epa_labor_market',
        'istac_employment_cnae',
        'istac_employment_divisions',
        'istac_employment_status',
        'foreign_affiliates_cnae',
        'tech_salaries',
        'emigration_pere'
    ];
BEGIN
    FOREACH dataset_name IN ARRAY datasets LOOP
        EXECUTE format($ddl$
            CREATE TABLE IF NOT EXISTS src.%I (
                row_id BIGSERIAL PRIMARY KEY,
                load_id UUID NOT NULL,
                row_number INTEGER NOT NULL,
                payload JSONB NOT NULL,
                record_origin VARCHAR(10) NOT NULL DEFAULT 'real'
                    CHECK (record_origin IN ('real', 'synthetic')),
                synthetic_run_id UUID,
                synthetic_period TEXT,
                synthetic_model VARCHAR(40),
                synthetic_layout VARCHAR(10)
                    CHECK (synthetic_layout IN ('long', 'wide') OR synthetic_layout IS NULL),
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        $ddl$, dataset_name || '_raw');
    END LOOP;
END
$$;
