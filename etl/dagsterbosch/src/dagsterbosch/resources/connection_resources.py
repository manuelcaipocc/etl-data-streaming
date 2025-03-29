from dagster import resource
from ConnectionManager import ConnectionManager  # Cambio importante
from utils import utils  # Cambio importante
import logging

logger = logging.getLogger(__name__)


@resource
def connection_manager_resource(_):
    cm = ConnectionManager()
    cm.connect_postgres()
    cm.connect_opcua()
    cm.connect_solace()
    yield cm 
    cm.disconnect_all() 
    return cm
