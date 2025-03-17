-- Crear la base de datos solo si no existe
DO
$$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'automax') THEN
      CREATE DATABASE automax;
   END IF;
END
$$;

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
          is_run_status BOOLEAN NOT NULL,    -- Nuevo campo agregado
          CONSTRAINT unique_entry UNIQUE (NamespaceIndex, RouteName, Timestamp, CtrlX_Name, Site) 
      );

      -- Crear índices para mejorar el rendimiento de las consultas
      CREATE INDEX IF NOT EXISTS idx_timestamp ON sandbox.ctrlx_data (Timestamp);
      CREATE INDEX IF NOT EXISTS idx_routename ON sandbox.ctrlx_data (RouteName);
      CREATE INDEX IF NOT EXISTS idx_ctrlx_name ON sandbox.ctrlx_data (CtrlX_Name);
   END IF;
END
$$;
