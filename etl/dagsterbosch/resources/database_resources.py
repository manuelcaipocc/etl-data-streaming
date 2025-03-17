import os
from dagster import resource, Definitions
import cx_Oracle

@resource(config_schema={})  # No se define config_schema porque usaremos variables de entorno
def oracle_connection(_):
    """Crea una conexión a la base de datos Oracle usando las variables de entorno existentes."""
    user = os.getenv("DB_USER", "system")  # Usuario por defecto
    password = os.getenv("DB_PASSWORD", "oracle")  # Contraseña por defecto
    host = os.getenv("DB_HOST", "localhost")  # Host por defecto
    port = os.getenv("DB_PORT", "1521")  # Puerto por defecto
    service = os.getenv("DB_NAME", "XE")  # Nombre de la base de datos

    dsn = f"{host}:{port}/{service}"  # Formato esperado por cx_Oracle
    return cx_Oracle.connect(user, password, dsn)

# 🔹 Registrar el recurso en Dagster para que sea reconocido
defs = Definitions(resources={"oracle_connection": oracle_connection})
