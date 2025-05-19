-- Create the database only if it does not exist
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

-- Create the schema and table within "automax"
DO
$$
BEGIN
-- Execute only if the current database is "automax"
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

        -- Create indexes to improve query performance
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


        -- Create the signals table        --signals

        CREATE TABLE sandbox.ctrlx_signals (
        code TEXT PRIMARY KEY,               -- Ej. 'P_A'
        browsename TEXT NOT NULL,           -- Ej. 'P_a'
        ctrlx_name TEXT,                    -- Ej. 'CtrlX_2'
        site TEXT,                          -- Ej. 'Cube-Ulm'
        is_run_status BOOLEAN,
        datatype TEXT,
        table_storage VARCHAR(20) NOT NULL,
        frequency TEXT
        );
        
    END IF;
END
$$;


-- Incluir funciones desde archivo externo
--\i /docker-entrypoint-initdb.d/update_pivot_functions.sql

--------------------------------------------------------------
-- Function to reorder columns in a table (timestamp first, then alphabetical)
CREATE OR REPLACE FUNCTION sandbox.reorder_table_columns(table_name TEXT)
RETURNS void AS $$
DECLARE
    new_columns TEXT;
    current_columns TEXT[];
BEGIN
    -- Get current columns in the table
    SELECT array_agg(cols.column_name::text ORDER BY 
                    CASE WHEN cols.column_name = 'timestamp' THEN 0 ELSE 1 END,
                    cols.column_name)
    INTO current_columns
    FROM information_schema.columns AS cols
    WHERE cols.table_schema = 'sandbox'
    AND cols.table_name = split_part(reorder_table_columns.table_name, '.', 2);
    
    -- Build the new column list
    SELECT string_agg(quote_ident(col), ', ')
    INTO new_columns
    FROM unnest(current_columns) AS col;
    
    -- Only proceed if we have columns
    IF new_columns IS NOT NULL THEN
        -- Create a temporary table with the new order
        EXECUTE format('CREATE TEMP TABLE temp_reorder AS SELECT %s FROM %s', new_columns, table_name);
        
        -- Truncate the original table
        EXECUTE format('TRUNCATE TABLE %s', table_name);
        
        -- Insert back with new order
        EXECUTE format('INSERT INTO %s SELECT %s FROM temp_reorder', table_name, new_columns);
        
        -- Drop the temporary table
        EXECUTE 'DROP TABLE temp_reorder';
        
        RAISE NOTICE 'Columns in table % reordered successfully', table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;


--------------------------------------------------------------
-- Function to initialize columns in a pivot table for a given frequency

CREATE OR REPLACE FUNCTION sandbox.initialize_columns_for_frequency(frequency TEXT)
RETURNS void AS $$
DECLARE
    pivot_table TEXT;
    r RECORD;
    col_exists BOOLEAN;
    pivot_table_exists BOOLEAN;
    column_list TEXT[];
    col_name TEXT;
    column_datatype TEXT;
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

    -- First collect all column names to add, sorted alphabetically
    SELECT array_agg(code ORDER BY code)
    INTO column_list
    FROM sandbox.ctrlx_signals
    WHERE (ctrlx_signals.frequency = '500mS' AND table_storage = '500mS')
    OR (ctrlx_signals.frequency = '1S' AND table_storage IN ('1S', '500mS'))
    OR (ctrlx_signals.frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'));

    -- Add columns in alphabetical order
    IF column_list IS NOT NULL THEN
        FOREACH col_name IN ARRAY column_list LOOP
            -- Get the datatype for this column
            SELECT datatype INTO column_datatype
            FROM sandbox.ctrlx_signals
            WHERE code = col_name LIMIT 1;

            -- Check if the column already exists
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'sandbox'
                AND table_name = split_part(pivot_table, '.', 2)
                AND column_name = col_name
            ) INTO col_exists;

            -- If not, add it
            IF NOT col_exists THEN
                RAISE NOTICE 'Adding column % to table %', col_name, pivot_table;

                EXECUTE format(
                    'ALTER TABLE %s ADD COLUMN %I %s',
                    pivot_table,
                    col_name,
                    CASE column_datatype
                        WHEN 'Float' THEN 'DOUBLE PRECISION'
                        WHEN 'Boolean' THEN 'DOUBLE PRECISION'
                        ELSE 'TEXT'
                    END
                );
            ELSE
                RAISE NOTICE 'Column % already exists in table %', col_name, pivot_table;
            END IF;
        END LOOP;
    END IF;

    RAISE NOTICE 'Reordering columns in pivot table %...', pivot_table;
    
    -- Reorder all columns (timestamp first, then others alphabetically)
    PERFORM sandbox.reorder_table_columns(pivot_table);

    RAISE NOTICE 'Completed initialize_columns_for_frequency for frequency: %', frequency;
END;
$$ LANGUAGE plpgsql;


--------------------------------------------------------------
-- Function to initialize columns in a temporary table for a given frequency
CREATE OR REPLACE FUNCTION sandbox.initialize_tmp_columns_for_frequency(frequency TEXT)
RETURNS void AS $$
DECLARE
    tmp_table TEXT;
    col_datatype TEXT;  -- Variable renombrada
    col_exists BOOLEAN;
    column_list TEXT[];
    current_column_name TEXT;  -- Variable renombrada
    tmp_table_name TEXT;
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

    -- Get just the table name without schema
    tmp_table_name := split_part(tmp_table, '.', 2);

    -- First collect all column names to add, sorted alphabetically
    SELECT array_agg(code ORDER BY code)
    INTO column_list
    FROM sandbox.ctrlx_signals
    WHERE (ctrlx_signals.frequency = '500mS' AND table_storage = '500mS')
       OR (ctrlx_signals.frequency = '1S' AND table_storage IN ('1S', '500mS'))
       OR (ctrlx_signals.frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'));

    -- Add columns in alphabetical order
    IF column_list IS NOT NULL THEN
        FOREACH current_column_name IN ARRAY column_list LOOP
            -- Get the datatype for this column (qualified with table alias)
            SELECT s.datatype INTO col_datatype
            FROM sandbox.ctrlx_signals s
            WHERE s.code = current_column_name 
            LIMIT 1;
            
            -- Check if the column already exists (qualified column reference)
            SELECT EXISTS (
                SELECT 1 
                FROM information_schema.columns cols
                WHERE cols.table_schema = 'sandbox'
                  AND cols.table_name = tmp_table_name
                  AND cols.column_name = current_column_name
            ) INTO col_exists;

            IF NOT col_exists THEN
                RAISE NOTICE 'Adding column % to %', current_column_name, tmp_table;
                EXECUTE format('ALTER TABLE %s ADD COLUMN %I %s',
                    tmp_table,
                    current_column_name,
                    CASE col_datatype
                        WHEN 'Float' THEN 'DOUBLE PRECISION'
                        WHEN 'Boolean' THEN 'DOUBLE PRECISION'
                        ELSE 'TEXT'
                    END
                );
            END IF;
        END LOOP;
    END IF;

    RAISE NOTICE 'Reordering columns in temporary table %...', tmp_table;
    
    -- Reorder all columns (timestamp first, then others alphabetically)
    PERFORM sandbox.reorder_table_columns(tmp_table);
END;
$$ LANGUAGE plpgsql;



-----------------------------------------------------------------------
-- Function to pivot data from ctrlx_data into a temporary table for a given frequency
CREATE OR REPLACE FUNCTION sandbox.pivot_ctrlx_data(frequency TEXT)
RETURNS void AS $$
DECLARE
    ts_expression TEXT;
    tmp_table TEXT;
    pivot_table TEXT;
    signals TEXT;
    tmp_table_exists BOOLEAN;
    pivot_table_exists BOOLEAN;
    column_list TEXT;
    last_timestamp TIMESTAMP;
    where_clause TEXT;
    row_count INT;
BEGIN
    RAISE NOTICE 'Starting pivot_ctrlx_data with input frequency: %', frequency;

    -- Determine the timestamp expression and temporary/target table
    IF frequency = '500mS' THEN
        ts_expression := 'date_trunc(''second'', timestamp) + floor(extract(milliseconds from timestamp)::int / 500) * interval ''0.5 second''';
        tmp_table := 'sandbox.tmp_ctrlx_new_500ms';
        pivot_table := 'sandbox.ctrlx_pivot_500ms';
    ELSIF frequency = '1S' THEN
        ts_expression := 'date_trunc(''second'', timestamp)';
        tmp_table := 'sandbox.tmp_ctrlx_new_1s';
        pivot_table := 'sandbox.ctrlx_pivot_1s';
    ELSIF frequency = '1M' THEN
        ts_expression := 'date_trunc(''minute'', timestamp)';
        tmp_table := 'sandbox.tmp_ctrlx_new_1min';
        pivot_table := 'sandbox.ctrlx_pivot_1min';
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

    -- Check if the pivot table exists before selecting MAX(timestamp)
    SELECT EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(pivot_table, '.', 2)
    ) INTO pivot_table_exists;

    IF pivot_table_exists THEN
        EXECUTE format('SELECT MAX(timestamp) FROM %s', pivot_table) INTO last_timestamp;
    ELSE
        RAISE NOTICE 'Pivot table % does not exist. Assuming first run.', pivot_table;
        last_timestamp := NULL;
    END IF;

    -- Determine the WHERE clause
    IF last_timestamp IS NULL THEN
        where_clause := '1=1';
        RAISE NOTICE 'First run for frequency %, processing all historical data', frequency;
    ELSE
        where_clause := format('timestamp > %L', last_timestamp);
        RAISE NOTICE 'Processing new data since % for frequency %', last_timestamp, frequency;
    END IF;

    -- Check if ctrlx_data has relevant data
    EXECUTE format('SELECT COUNT(*) FROM sandbox.ctrlx_data WHERE %s', where_clause) INTO row_count;
    IF row_count = 0 THEN
        RAISE NOTICE 'No data available in ctrlx_data for frequency %, skipping pivoting step.', frequency;
        RETURN;
    END IF;

    RAISE NOTICE 'Truncating temporary table % before data aggregation.', tmp_table;
    EXECUTE format('TRUNCATE TABLE %s', tmp_table);

    RAISE NOTICE 'Generating signal columns for aggregation.';

    -- Build the aggregation expression dynamically with alphabetical order
    WITH signal_codes AS (
        SELECT code
        FROM sandbox.ctrlx_signals
        WHERE (ctrlx_signals.frequency = '500mS' AND table_storage = '500mS')
           OR (ctrlx_signals.frequency = '1S' AND table_storage IN ('1S', '500mS'))
           OR (ctrlx_signals.frequency = '1M' AND table_storage IN ('1M', '1S', '500mS'))
        GROUP BY code
        ORDER BY code
    )
    SELECT string_agg(
        format('MAX(value) FILTER (WHERE code = %L) AS %I', code, code), ', '
    )
    INTO signals
    FROM signal_codes;

    IF signals IS NULL THEN
        RAISE NOTICE 'No signals defined for frequency %, skipping data insertion.', frequency;
        RETURN;
    END IF;

    RAISE NOTICE 'Signal aggregation expression generated. Inserting data into %', tmp_table;

    EXECUTE format(
        'INSERT INTO %s SELECT %s AS timestamp, %s
         FROM sandbox.ctrlx_data
         WHERE %s
         GROUP BY 1 ORDER BY 1',
        tmp_table, ts_expression, signals, where_clause
    );

    RAISE NOTICE 'Completed pivot_ctrlx_data for frequency: %', frequency;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------
-- Function to ensure that the pivot table has all the necessary columns from the source table
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
-- Function to synchronize data from the temporary pivot table to the main pivot table
CREATE OR REPLACE FUNCTION sandbox.sync_ctrlx_pivot_table(frequency TEXT)
RETURNS void AS $$
DECLARE
    tmp_table TEXT;
    pivot_table TEXT;
    tmp_table_exists BOOLEAN;
    row_count INTEGER;
    column_list TEXT;
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

    -- Check if temporary table has rows
    EXECUTE format('SELECT COUNT(*) FROM %s', tmp_table) INTO row_count;

    IF row_count = 0 THEN
        RAISE NOTICE 'Temporary table % is empty. Skipping data insertion.', tmp_table;
        RETURN;
    END IF;

    RAISE NOTICE 'Ensuring pivot table has all necessary columns...';
    -- Ensure the main pivot table has all columns present in the temporary table
    PERFORM sandbox.ensure_columns_exist(pivot_table, tmp_table);

    -- Get ordered column list (timestamp first, then others alphabetically)
    SELECT string_agg(quote_ident(column_name), ', ')
    INTO column_list
    FROM (
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'sandbox'
        AND table_name = split_part(tmp_table, '.', 2)
        ORDER BY CASE WHEN column_name = 'timestamp' THEN 0 ELSE 1 END,
                 column_name
    ) cols;

    RAISE NOTICE 'Inserting new data into pivot table % with column order: %', pivot_table, column_list;
    
    -- Insert new data from the temporary table to the main pivot table with explicit column order
    EXECUTE format(
        'INSERT INTO %s (%s)
         SELECT %s
         FROM %s t
         WHERE NOT EXISTS (
             SELECT 1 FROM %s p WHERE p.timestamp = t.timestamp
         )',
        pivot_table, column_list, column_list, tmp_table, pivot_table
    );

    -- Reorder columns in the pivot table to maintain consistency
    PERFORM sandbox.reorder_table_columns(pivot_table);

    RAISE NOTICE 'sync_ctrlx_pivot_table for % completed successfully.', frequency;
END;
$$ LANGUAGE plpgsql;



--Unique Variables 
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION sandbox.refresh_ctrlx_signals()
RETURNS void AS $$
BEGIN
    INSERT INTO sandbox.ctrlx_signals (
        code, browsename, ctrlx_name, site, is_run_status, datatype, table_storage, frequency)
    SELECT DISTINCT 
        code, browsename, ctrlx_name, site, is_run_status, datatype, table_storage,
        CASE 
            WHEN table_storage = '500mS' THEN '500mS'
            WHEN table_storage = '1S' THEN '1S'
            WHEN table_storage = '1M' THEN '1M'
            ELSE NULL
        END
    FROM sandbox.ctrlx_data
    ON CONFLICT (code) DO UPDATE
        SET browsename = EXCLUDED.browsename,
            ctrlx_name = EXCLUDED.ctrlx_name,
            site = EXCLUDED.site,
            is_run_status = EXCLUDED.is_run_status,
            datatype = EXCLUDED.datatype,
            table_storage = EXCLUDED.table_storage,
            frequency = EXCLUDED.frequency;

    RAISE NOTICE 'Table Signales updated including frequency...';       
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
