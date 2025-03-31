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
   END IF;
END
$$;

-- Crear tablas pivoteadas (solo estructura)
CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_500ms (
    timestamp TIMESTAMP PRIMARY KEY
    -- columnas dinámicas se agregarán luego con ALTER TABLE
);

CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_1s (
    timestamp TIMESTAMP PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS sandbox.ctrlx_pivot_1min (
    timestamp TIMESTAMP PRIMARY KEY
);

-- Crear tablas temporales para datos nuevos
CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_500ms (
    LIKE sandbox.ctrlx_data INCLUDING ALL
);

CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_1s (
    LIKE sandbox.ctrlx_data INCLUDING ALL
);

CREATE TABLE IF NOT EXISTS sandbox.tmp_ctrlx_new_1min (
    LIKE sandbox.ctrlx_data INCLUDING ALL
);


-- Incluir funciones desde archivo externo
\i /docker-entrypoint-initdb.d/update_pivot_functions.sql
