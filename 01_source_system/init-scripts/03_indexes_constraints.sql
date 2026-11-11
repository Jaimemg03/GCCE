DO $$
DECLARE
    dataset_name text;
    table_name text;
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
        table_name := dataset_name || '_raw';

        EXECUTE format(
            'CREATE UNIQUE INDEX IF NOT EXISTS %I ON src.%I (load_id, row_number)',
            'uq_' || table_name || '_load_row',
            table_name
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON src.%I (load_id)',
            'idx_' || table_name || '_load_id',
            table_name
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON src.%I (record_origin)',
            'idx_' || table_name || '_origin',
            table_name
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON src.%I (synthetic_run_id)',
            'idx_' || table_name || '_run',
            table_name
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS %I ON src.%I (loaded_at)',
            'idx_' || table_name || '_loaded_at',
            table_name
        );
    END LOOP;
END
$$;
