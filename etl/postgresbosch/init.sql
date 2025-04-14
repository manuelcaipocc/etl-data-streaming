-- Crear la base de datos solo si no existe
DO
$$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'automax') THEN
        CREATE DATABASE automax;
    END IF;
END
$$;

\connect automax
CREATE EXTENSION IF NOT EXISTS pg_cron;
ALTER SYSTEM SET cron.database_name = 'automax';

-- Crear el esquema y la tabla dentro de "automax"
DO
$$
BEGIN
-- Solo ejecutar si estamos dentro de "automax"
    IF current_database() = 'automax' THEN
        CREATE SCHEMA IF NOT EXISTS sandbox;

        CREATE TABLE IF NOT EXISTS sandbox.ctrlx_data (
            id BIGSERIAL PRIMARY KEY,
            NamespaceIndex INT NOT NULL, 
            RouteName VARCHAR(255) NOT NULL, 
            BrowseName VARCHAR(100) NOT NULL,
            Value NUMERIC(15,6) NOT NULL,  
            DataType VARCHAR(50) NOT NULL,
            Timestamp TIMESTAMP NOT NULL DEFAULT now(),
            Code VARCHAR(50) NOT NULL,
            CtrlX_Name VARCHAR(100) NOT NULL,  -- Nuevo campo agregado
            Site VARCHAR(100) NOT NULL,        -- Nuevo campo agregado
            is_run_status BOOLEAN NOT NULL,    -- Nuevo campo agregado,
            table_storage VARCHAR(20) NOT NULL,
            CONSTRAINT unique_entry UNIQUE (NamespaceIndex, RouteName, Timestamp, CtrlX_Name, Site) 
        );

        -- Crear índices para mejorar el rendimiento de las consultas
        CREATE INDEX IF NOT EXISTS idx_timestamp ON sandbox.ctrlx_data (Timestamp);
        CREATE INDEX IF NOT EXISTS idx_routename ON sandbox.ctrlx_data (RouteName);
        CREATE INDEX IF NOT EXISTS idx_ctrlx_name ON sandbox.ctrlx_data (CtrlX_Name);
        CREATE INDEX IF NOT EXISTS idx_ctrlx_data_code ON sandbox.ctrlx_data (Code);
        
        -- tables pivot
        CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_500ms (
            timestamp TIMESTAMP PRIMARY KEY );

        CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_1s (
            timestamp TIMESTAMP PRIMARY KEY);

        CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_1min (
            timestamp TIMESTAMP PRIMARY KEY);

        -- Temporal tables (sin 'id', solo para pivoting)
        CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_500ms (
            timestamp TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_1s (
            timestamp TIMESTAMP NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_1min (
            timestamp TIMESTAMP NOT NULL
        );


        --signals

        CREATE TABLE sandbox.ctrlx_signals (
        code TEXT PRIMARY KEY,               -- Ej. 'P_A'
        browsename TEXT NOT NULL,           -- Ej. 'P_a'
        ctrlx_name TEXT,                    -- Ej. 'CtrlX_2'
        site TEXT,                          -- Ej. 'Cube-Ulm'
        is_run_status BOOLEAN,
        datatype TEXT,
        table_storage VARCHAR(20) NOT NULL
        );
        
    END IF;
END
$$;


-- Incluir funciones desde archivo externo
--\i /docker-entrypoint-initdb.d/update_pivot_functions.sql

--------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.initialize_columns_for_frequency(frequency TEXT)
RETURNS void AS $$
DECLARE
    pivot_table TEXT;
    r RECORD;
    col_exists BOOLEAN;
    pivot_table_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting initialize_columns_for_frequency with input: %', frequency;

    -- Determine the pivot table
    IF frequency = '500mS' THEN
        pivot_table := 'sandbox.ctrlx_pivot_500ms';
    ELSIF frequency = '1S' THEN
        pivot_table := 'sandbox.ctrlx_pivot_1s';
    ELSIF frequency = '1M' THEN
        pivot_table := 'sandbox.ctrlx_pivot_1min';
    ELSE
        RAISE EXCEPTION 'Invalid frequency: %', frequency;
    END IF;

    RAISE NOTICE 'Target pivot table: %', pivot_table;

    -- Check if the pivot table exists
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(pivot_table, '.', 2)
    ) INTO pivot_table_exists;

    IF NOT pivot_table_exists THEN
        RAISE NOTICE 'Pivot table % does not exist. Skipping column initialization.', pivot_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Pivot table exists. Beginning column check and creation...';

    -- Iterate over relevant signals
    FOR r IN
        SELECT code, datatype FROM sandbox.ctrlx_signals
        WHERE (frequency = '500mS' AND table_storage = '500mS')
        OR (frequency = '1S' AND table_storage IN ('1S', '500mS'))
        OR (frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'))
    LOOP
        -- Check if the column already exists
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'sandbox'
            AND table_name = split_part(pivot_table, '.', 2)
            AND column_name = r.code
        ) INTO col_exists;

        -- If not, add it
        IF NOT col_exists THEN
            RAISE NOTICE 'Adding column % to table %', r.code, pivot_table;

            EXECUTE format(
                'ALTER TABLE %s ADD COLUMN %I %s',
                pivot_table,
                r.code,
                CASE r.datatype
                    WHEN 'Float' THEN 'DOUBLE PRECISION'
                    WHEN 'Boolean' THEN 'DOUBLE PRECISION'
                    ELSE 'TEXT'
                END
            );

            RAISE NOTICE 'Column % added. Updating existing rows with NULLs.', r.code;

            EXECUTE format(
                'UPDATE %s SET %I = NULL WHERE %I IS NULL',
                pivot_table, r.code, r.code
            );
        ELSE
            RAISE NOTICE 'Column % already exists in table %', r.code, pivot_table;
        END IF;
    END LOOP;

    RAISE NOTICE 'Completed initialize_columns_for_frequency for frequency: %', frequency;
END;
$$ LANGUAGE plpgsql;



--------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.initialize_tmp_columns_for_frequency(frequency TEXT)
RETURNS void AS $$
DECLARE
    tmp_table TEXT;
    r RECORD;
    col_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Initializing temporary columns for frequency: %', frequency;

    -- Define the correct tmp table
    IF frequency = '500mS' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_500ms';
    ELSIF frequency = '1S' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_1s';
    ELSIF frequency = '1M' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_1min';
    ELSE
        RAISE EXCEPTION 'Invalid frequency: %', frequency;
    END IF;

    -- Add missing columns
    FOR r IN
        SELECT code, datatype FROM sandbox.ctrlx_signals
        WHERE (frequency = '500mS' AND table_storage = '500mS')
           OR (frequency = '1S' AND table_storage IN ('1S', '500mS'))
           OR (frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'))
    LOOP
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'sandbox'
            AND table_name = split_part(tmp_table, '.', 2)
            AND column_name = r.code
        ) INTO col_exists;

        IF NOT col_exists THEN
            RAISE NOTICE 'Adding column % to %', r.code, tmp_table;
            EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s',
                tmp_table,
                r.code,
                CASE r.datatype
                    WHEN 'Float' THEN 'DOUBLE PRECISION'
                    WHEN 'Boolean' THEN 'DOUBLE PRECISION'
                    ELSE 'TEXT'
                END
            );
        END IF;
    END LOOP;

END;
$$ LANGUAGE plpgsql;


-----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.pivot_ctrlx_data(frequency TEXT)
RETURNS void AS $$
DECLARE
    ts_expression TEXT;
    tmp_table TEXT;
    signals TEXT;
    tmp_table_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting pivot_ctrlx_data with input frequency: %', frequency;

    -- Determine the timestamp expression and temporary table
    IF frequency = '500mS' THEN
        ts_expression := 'date_trunc(''second'', timestamp) + floor(extract(milliseconds from timestamp)::int / 500) * interval ''0.5 second''';
        tmp_table := 'sandbox.tmp_ctrlx_new_500ms';
    ELSIF frequency = '1S' THEN
        ts_expression := 'date_trunc(''second'', timestamp)';
        tmp_table := 'sandbox.tmp_ctrlx_new_1s';
    ELSIF frequency = '1M' THEN
        ts_expression := 'date_trunc(''minute'', timestamp)';
        tmp_table := 'sandbox.tmp_ctrlx_new_1min';
    ELSE
        RAISE EXCEPTION 'Invalid frequency: %', frequency;
    END IF;

    RAISE NOTICE 'Using temporary table: %', tmp_table;

    -- Check if the temporary table exists
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(tmp_table, '.', 2)
    ) INTO tmp_table_exists;

    IF NOT tmp_table_exists THEN
        RAISE NOTICE 'Temporary table % does not exist. Skipping pivot_ctrlx_data.', tmp_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Truncating temporary table % before data aggregation.', tmp_table;

    -- Truncate the temp table
    EXECUTE format('TRUNCATE TABLE %s', tmp_table);

    RAISE NOTICE 'Generating signal columns for aggregation.';

    -- Build the aggregation expression dynamically
    SELECT string_agg(
        format('MAX(value) FILTER (WHERE code = %L) AS %I', code, code), ', '
    )
    INTO signals
    FROM sandbox.ctrlx_signals
    WHERE (frequency = '500mS' AND table_storage = '500mS')
       OR (frequency = '1S' AND table_storage IN ('1S', '500mS'))
       OR (frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'));

    RAISE NOTICE 'Signal aggregation expression generated. Inserting data into %', tmp_table;

    -- Insert pivoted data
    EXECUTE format(
        'INSERT INTO %s SELECT %s AS timestamp, %s
         FROM sandbox.ctrlx_data
         WHERE timestamp > now() - interval ''5 minutes''
         GROUP BY 1 ORDER BY 1',
        tmp_table, ts_expression, signals
    );

    RAISE NOTICE 'Completed pivot_ctrlx_data for frequency: %', frequency;
END;
$$ LANGUAGE plpgsql;



------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.ensure_columns_exist(pivot_table TEXT, source_table TEXT)
RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    RAISE NOTICE 'Starting ensure_columns_exist: source table = %, target pivot table = %', source_table, pivot_table;

    FOR r IN
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(source_table, '.', 2)
        AND column_name <> 'timestamp'
        AND column_name NOT IN (
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'sandbox'
            AND table_name = split_part(pivot_table, '.', 2)
        )
    LOOP
        RAISE NOTICE 'Adding missing column % of type % to pivot table %', r.column_name, r.data_type, pivot_table;

        EXECUTE format(
            'ALTER TABLE %I ADD COLUMN IF NOT EXISTS %I %s',
            pivot_table, r.column_name, r.data_type
        );
    END LOOP;

    RAISE NOTICE 'Completed ensure_columns_exist for pivot table %', pivot_table;
END;
$$ LANGUAGE plpgsql;


----------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.sync_ctrlx_pivot_table(frequency TEXT)
RETURNS void AS $$
DECLARE
    tmp_table TEXT;
    pivot_table TEXT;
    tmp_table_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting sync_ctrlx_pivot_table with frequency: %', frequency;

    IF frequency = '500mS' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_500ms';
        pivot_table := 'sandbox.ctrlx_pivot_500ms';
    ELSIF frequency = '1S' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_1s';
        pivot_table := 'sandbox.ctrlx_pivot_1s';
    ELSIF frequency = '1M' THEN
        tmp_table := 'sandbox.tmp_ctrlx_new_1min';
        pivot_table := 'sandbox.ctrlx_pivot_1min';
    ELSE
        RAISE EXCEPTION 'Invalid frequency: %', frequency;
    END IF;

    RAISE NOTICE 'Temporary table: %, Pivot table: %', tmp_table, pivot_table;

    -- Check if the temporary table exists
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(tmp_table, '.', 2)
    ) INTO tmp_table_exists;

    IF NOT tmp_table_exists THEN
        RAISE NOTICE 'Temporary table % does not exist. Skipping sync_ctrlx_pivot_table.', tmp_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Ensuring pivot table has all necessary columns...';
    PERFORM sandbox.ensure_columns_exist(pivot_table, tmp_table);

    RAISE NOTICE 'Inserting new data into pivot table %...', pivot_table;
    EXECUTE format(
        'INSERT INTO %s
         SELECT *
         FROM %s t
         WHERE NOT EXISTS (
             SELECT 1 FROM %s p WHERE p.timestamp = t.timestamp
         )',
        pivot_table, tmp_table, pivot_table
    );

    RAISE NOTICE 'sync_ctrlx_pivot_table for % completed successfully.', frequency;
END;
$$ LANGUAGE plpgsql;


--Unique Variables 
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.refresh_ctrlx_signals()
RETURNS void AS $$
BEGIN
    INSERT INTO sandbox.ctrlx_signals (code, browsename, ctrlx_name, site, is_run_status, datatype,table_storage)
    SELECT DISTINCT code, browsename, ctrlx_name, site, is_run_status, datatype,table_storage
    FROM sandbox.ctrlx_data
    ON CONFLICT (code) DO UPDATE
        SET browsename = EXCLUDED.browsename,
            ctrlx_name = EXCLUDED.ctrlx_name,
            site = EXCLUDED.site,
            is_run_status = EXCLUDED.is_run_status,
            datatype = EXCLUDED.datatype,
            table_storage = EXCLUDED.table_storage;
    RAISE NOTICE 'Table Signales generated ...';       
END;
$$ LANGUAGE plpgsql;


-- SELECT cron.schedule(
--     'refresh_ctrlx_signals_every_4min',
--     '*/4 * * * *',
--     $$ SELECT sandbox.refresh_ctrlx_signals(); $$
-- );


-- SELECT cron.schedule(
--     'pivot_500ms', '*/5 * * * *',
--     $$ 
--     SELECT sandbox.initialize_columns_for_frequency('500mS');
--     SELECT sandbox.pivot_ctrlx_data('500mS');
--     SELECT sandbox.sync_ctrlx_pivot_table('500mS');
--     $$
-- );

-- SELECT cron.schedule(
--     'pivot_1S', '*/5 * * * *',
--     $$ 
--     SELECT sandbox.initialize_columns_for_frequency('1S');
--     SELECT sandbox.pivot_ctrlx_data('1S');
--     SELECT sandbox.sync_ctrlx_pivot_table('1S');
--     $$
-- );

-- SELECT cron.schedule(
--     'pivot_1M', '*/5 * * * *',
--     $$ 
--     SELECT sandbox.initialize_columns_for_frequency('1M');
--     SELECT sandbox.pivot_ctrlx_data('1M');
--     SELECT sandbox.sync_ctrlx_pivot_table('1M');
--     $$
-- );
