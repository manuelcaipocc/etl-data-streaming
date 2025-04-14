--cada segundo 

CREATE TABLE sandbox.ctrlx_data_pivoted_1s AS
SELECT 
  date_trunc('second', timestamp) AS ts,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_A') AS CtrlX_2_P_A,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_B') AS CtrlX_2_P_B,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_RENERG') AS CtrlX_1_RENERG,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_V') AS CtrlX_2_V,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_S') AS CtrlX_2_P_S,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_ACTIVE') AS CtrlX_1_ACTIVE,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_ENABLE') AS CtrlX_2_ENABLE
FROM sandbox.ctrlx_data
GROUP BY date_trunc('second', timestamp)
ORDER BY ts;


-- cada medio segundo

CREATE OR REPLACE VIEW sandbox.ctrlx_data_pivoted_500ms AS
SELECT 
  date_trunc('second', timestamp) + 
    FLOOR(EXTRACT(milliseconds FROM timestamp)::int / 500) * interval '0.5 second' AS ts,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_A') AS CtrlX_2_P_A,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_B') AS CtrlX_2_P_B,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_RENERG') AS CtrlX_1_RENERG,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_V') AS CtrlX_2_V,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_S') AS CtrlX_2_P_S,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_ACTIVE') AS CtrlX_1_ACTIVE,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_ENABLE') AS CtrlX_2_ENABLE
FROM sandbox.ctrlx_data
GROUP BY ts
ORDER BY ts;


--cada minuto

CREATE TABLE sandbox.ctrlx_data_pivoted_1m AS
SELECT 
  date_trunc('minute', timestamp) AS ts,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_A') AS CtrlX_2_P_A,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_B') AS CtrlX_2_P_B,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_RENERG') AS CtrlX_1_RENERG,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_V') AS CtrlX_2_V,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_P_S') AS CtrlX_2_P_S,
  MAX(value) FILTER (WHERE code = 'CtrlX_1_ACTIVE') AS CtrlX_1_ACTIVE,
  MAX(value) FILTER (WHERE code = 'CtrlX_2_ENABLE') AS CtrlX_2_ENABLE
FROM sandbox.ctrlx_data
GROUP BY date_trunc('minute', timestamp)
ORDER BY ts;

