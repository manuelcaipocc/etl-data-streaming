from dagster import resource
from ConnectionManager import ConnectionManager  # Cambio importante
from utils import utils  # Cambio importante
import logging

logger = logging.getLogger(__name__)


@resource
def postgres_resource(_):
    conn_manager = ConnectionManager()
    conn = conn_manager.connect_postgres()
    if conn is None:
        raise Exception("Could not connect to PostgreSQL.")
    return conn

@resource
def solace_resource(_):
    conn = ConnectionManager()
    solace_service = conn.connect_solace()
    if solace_service is None:
        raise Exception("Could not connect to Solace.")
    return solace_service

@resource
def opcua_resource(_):
    conn = ConnectionManager()
    conn.connect_opcua()
    return conn  # Aquí devuelves el gestor completo porque puedes usar `get_opcua_client()`
