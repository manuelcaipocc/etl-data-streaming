-- Helper: Get unique variable names from temporary table
CREATE OR REPLACE FUNCTION sandbox.get_variable_names(temp_table TEXT)
RETURNS TABLE (browsename TEXT) AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT DISTINCT browsename::TEXT FROM sandbox.%I', temp_table);
END;
$$ LANGUAGE plpgsql;

-- Helper: Ensure columns exist in pivot table
CREATE OR REPLACE FUNCTION sandbox.ensure_pivot_columns(pivot_table TEXT, temp_table TEXT)
RETURNS void AS $$
DECLARE
  var_name TEXT;
BEGIN
  FOR var_name IN SELECT * FROM sandbox.get_variable_names(temp_table)
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns 
      WHERE table_schema='sandbox' AND table_name=pivot_table AND column_name=var_name
    ) THEN
      EXECUTE format('ALTER TABLE sandbox.%I ADD COLUMN %I NUMERIC(15,6);', pivot_table, var_name);
    END IF;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Helper: Update pivot values
CREATE OR REPLACE FUNCTION sandbox.update_pivot_values(pivot_table TEXT, pivot_data_sql TEXT)
RETURNS void AS $$
DECLARE
  var_name TEXT;
BEGIN
  FOR var_name IN
    EXECUTE format('SELECT column_name FROM information_schema.columns WHERE table_schema = ''sandbox'' AND table_name = %L AND column_name != ''timestamp''', pivot_table)
  LOOP
    EXECUTE format(
      'UPDATE sandbox.%I SET "%1$I" = (d.data->>%1$L)::numeric FROM (%s) AS d WHERE sandbox.%I.timestamp = d.timestamp;',
      pivot_table, pivot_data_sql, pivot_table
    );
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: update_pivot_500ms
CREATE OR REPLACE FUNCTION sandbox.update_pivot_500ms()
RETURNS void AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP := now();
  pivot_sql TEXT;
BEGIN
  SELECT COALESCE(MAX(timestamp), (SELECT MIN(timestamp) FROM sandbox.ctrlx_data))
  INTO start_time
  FROM sandbox.ctrlx_pivot_500ms;

  DELETE FROM sandbox.tmp_ctrlx_new_500ms;
  INSERT INTO sandbox.tmp_ctrlx_new_500ms
  SELECT * FROM sandbox.ctrlx_data
  WHERE timestamp > start_time AND timestamp <= end_time AND table_storage = '500mS';

  PERFORM sandbox.ensure_pivot_columns('ctrlx_pivot_500ms', 'tmp_ctrlx_new_500ms');

  WITH time_series AS (
    SELECT generate_series(start_time, end_time, '500 milliseconds') AS ts
  ), vars AS (
    SELECT * FROM sandbox.get_variable_names('tmp_ctrlx_new_500ms')
  ), last_values AS (
    SELECT ts.ts, v.browsename,
           (SELECT CASE 
                       WHEN d.is_run_status THEN CASE WHEN d.value::text = 'true' THEN 1 ELSE 0 END
                       ELSE d.value::numeric 
                   END
            FROM sandbox.tmp_ctrlx_new_500ms d
            WHERE d.browsename = v.browsename AND d.timestamp <= ts.ts
            ORDER BY d.timestamp DESC LIMIT 1) AS value
    FROM time_series ts
    CROSS JOIN vars v
  ),
  pivot_data AS (
    SELECT ts AS timestamp, jsonb_object_agg(browsename, value) AS data FROM last_values GROUP BY ts
  )
  INSERT INTO sandbox.ctrlx_pivot_500ms(timestamp)
  SELECT p.timestamp FROM pivot_data p
  ON CONFLICT (timestamp) DO NOTHING;

  SELECT 'SELECT timestamp, data FROM pivot_data' INTO pivot_sql;
  PERFORM sandbox.update_pivot_values('ctrlx_pivot_500ms', pivot_sql);
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: update_pivot_1s
CREATE OR REPLACE FUNCTION sandbox.update_pivot_1s()
RETURNS void AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP := now();
  pivot_sql TEXT;
BEGIN
  SELECT COALESCE(MAX(timestamp), (SELECT MIN(timestamp) FROM sandbox.ctrlx_data))
  INTO start_time
  FROM sandbox.ctrlx_pivot_1s;

  DELETE FROM sandbox.tmp_ctrlx_new_1s;
  INSERT INTO sandbox.tmp_ctrlx_new_1s
  SELECT * FROM sandbox.ctrlx_data
  WHERE timestamp > start_time AND timestamp <= end_time AND table_storage IN ('500mS', '1S');

  PERFORM sandbox.ensure_pivot_columns('ctrlx_pivot_1s', 'tmp_ctrlx_new_1s');

  WITH time_series AS (
    SELECT generate_series(start_time, end_time, '1 second') AS ts
  ), vars AS (
    SELECT * FROM sandbox.get_variable_names('tmp_ctrlx_new_1s')
  ), last_values AS (
    SELECT ts.ts, v.browsename,
           (SELECT CASE 
                       WHEN d.is_run_status THEN CASE WHEN d.value::text = 'true' THEN 1 ELSE 0 END
                       ELSE d.value::numeric 
                   END
            FROM sandbox.tmp_ctrlx_new_1s d
            WHERE d.browsename = v.browsename AND d.timestamp <= ts.ts
            ORDER BY d.timestamp DESC LIMIT 1) AS value
    FROM time_series ts
    CROSS JOIN vars v
  ),
  pivot_data AS (
    SELECT ts AS timestamp, jsonb_object_agg(browsename, value) AS data FROM last_values GROUP BY ts
  )
  INSERT INTO sandbox.ctrlx_pivot_1s(timestamp)
  SELECT p.timestamp FROM pivot_data p
  ON CONFLICT (timestamp) DO NOTHING;

  SELECT 'SELECT timestamp, data FROM pivot_data' INTO pivot_sql;
  PERFORM sandbox.update_pivot_values('ctrlx_pivot_1s', pivot_sql);
END;
$$ LANGUAGE plpgsql;

-- FUNCTION: update_pivot_1min
CREATE OR REPLACE FUNCTION sandbox.update_pivot_1min()
RETURNS void AS $$
DECLARE
  start_time TIMESTAMP;
  end_time TIMESTAMP := now();
  pivot_sql TEXT;
BEGIN
  SELECT COALESCE(MAX(timestamp), (SELECT MIN(timestamp) FROM sandbox.ctrlx_data))
  INTO start_time
  FROM sandbox.ctrlx_pivot_1min;

  DELETE FROM sandbox.tmp_ctrlx_new_1min;
  INSERT INTO sandbox.tmp_ctrlx_new_1min
  SELECT * FROM sandbox.ctrlx_data
  WHERE timestamp > start_time AND timestamp <= end_time AND table_storage IN ('500mS', '1S', '1M');

  PERFORM sandbox.ensure_pivot_columns('ctrlx_pivot_1min', 'tmp_ctrlx_new_1min');

  WITH time_series AS (
    SELECT generate_series(start_time, end_time, '1 second') AS ts
  ), vars AS (
    SELECT * FROM sandbox.get_variable_names('tmp_ctrlx_new_1min')
  ), last_values AS (
    SELECT ts.ts, v.browsename,
           (SELECT CASE 
                       WHEN d.is_run_status THEN CASE WHEN d.value::text = 'true' THEN 1 ELSE 0 END
                       ELSE d.value::numeric 
                   END
            FROM sandbox.tmp_ctrlx_new_1min d
            WHERE d.browsename = v.browsename AND d.timestamp <= ts.ts
            ORDER BY d.timestamp DESC LIMIT 1) AS value
    FROM time_series ts
    CROSS JOIN vars v
  ),
  pivot_data AS (
    SELECT ts AS timestamp, jsonb_object_agg(browsename, value) AS data FROM last_values GROUP BY ts
  )
  INSERT INTO sandbox.ctrlx_pivot_1min(timestamp)
  SELECT p.timestamp FROM pivot_data p
  ON CONFLICT (timestamp) DO NOTHING;

  SELECT 'SELECT timestamp, data FROM pivot_data' INTO pivot_sql;
  PERFORM sandbox.update_pivot_values('ctrlx_pivot_1min', pivot_sql);
END;
$$ LANGUAGE plpgsql;

-- Programar tareas automáticas de pivoteo
SELECT cron.schedule('pivoteo_500ms', '*/5 * * * *', $$ SELECT sandbox.update_pivot_500ms(); $$);
SELECT cron.schedule('pivoteo_1s',     '*/5 * * * *', $$ SELECT sandbox.update_pivot_1s(); $$);
SELECT cron.schedule('pivoteo_1min',   '*/5 * * * *', $$ SELECT sandbox.update_pivot_1min(); $$);