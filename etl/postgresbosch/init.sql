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
          NamespaceIndex INT NOT NULL, -- Nuevo campo agregado
          RouteName VARCHAR(255) NOT NULL, -- Nuevo campo agregado
          BrowseName VARCHAR(100) NOT NULL,
          Value NUMERIC(15,6) NOT NULL,  -- Aumentar precisión decimal
          DataType VARCHAR(50) NOT NULL,
          Timestamp TIMESTAMP NOT NULL DEFAULT now(),
          Code VARCHAR(50) NOT NULL,
          CONSTRAINT unique_entry UNIQUE (NamespaceIndex, RouteName, Timestamp) -- Garantiza no duplicados
      );

      -- Crear índice en el Timestamp para mejorar rendimiento en consultas por tiempo
      CREATE INDEX IF NOT EXISTS idx_timestamp ON sandbox.ctrlx_data (Timestamp);
   END IF;
END
$$;
