import json
import logging
import time
import yaml
import threading
from opcua import Client, ua
from datetime import datetime
from pathlib import Path
import solace.messaging.messaging_service as messaging_service
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from utils import utils
from ConnectionManager import ConnectionManager
from concurrent.futures import ThreadPoolExecutor
import threading

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = utils.load_config()
last_values = {}

def create_publisher(service):
    """Creates a direct publisher for Solace"""
    if service is None or not service.is_connected:
        logger.error("No active connection with Solace. Cannot create publisher.")
        return None

    try:
        publisher = service.create_direct_message_publisher_builder().build()
        publisher.start()
        logger.info("Solace publisher initialized successfully.")
        return publisher
    except Exception as e:
        logger.error(f"Error creating Solace publisher: {e}")
        return None

# Publish data to Solace efficiently
def publish_to_solace(publisher, topic_name, payload):
    """Publishes a message to Solace on the specified topic"""
    if publisher is None or not publisher.is_ready():
        logger.error("No active connection with the Solace publisher. Cannot publish.")
        return

    try:
        topic = Topic.of(topic_name)
        message = json.dumps(payload)
        publisher.publish(message, topic)
        logger.info(f"Published to Solace ({topic_name}): {payload}")
    except Exception as e:
        logger.error(f"Error sending message to Solace: {e}")

def extract_sensor_data(opcua_client, solace_publisher, node):
    """Extrae datos de un sensor y los envía a Solace solo si hay una variación significativa"""
    browse_name = node["BrowseName"]
    namespace_index = node.get("NamespaceIndex", 2)
    route_name = node["RouteName"]
    update_interval = node.get("update_interval", 100) / 1000  # Convertir a segundos
    variation_threshold = node.get("variation_threshold", 0.01)  # Default: 1% de variación mínima

    node_id = f"ns={namespace_index};s={route_name}"
    last_value = last_values.get(node_id, None)

    while True:
        try:
            opcua_node = opcua_client.get_node(node_id)
            value = opcua_node.get_value()
            timestamp = datetime.utcnow().isoformat() + "Z"

            if last_value is not None:
                delta = abs(value - last_value) / (abs(last_value) + 1e-9)  # Evita divisiones por cero
                if delta < variation_threshold:
                    time.sleep(update_interval)
                    continue  # No publica si la variación es menor al umbral

            last_values[node_id] = value

            variable_data = {
                "NamespaceIndex": namespace_index,
                "RouteName": route_name,
                "BrowseName": browse_name,
                "Value": value,
                "DataType": node.get("type", "Unknown"),
                "processed": False,
                "Timestamp": timestamp
            }

            publish_to_solace(solace_publisher, config["solace"]["topics"]["raw_data"][0], variable_data)

        except Exception as e:
            logger.error(f"Error extrayendo {browse_name} ({route_name}): {e}")

        time.sleep(update_interval)

def extract_data():
    """Inicializa las conexiones OPC UA y Solace y lanza hilos para extraer datos de cada sensor"""
    connection_manager = ConnectionManager()  # Se crea la única instancia del singleton
    connection_manager.connect_opcua()  # Inicia todas las conexiones OPC UA

    logger.info("Esperando 10 segundos antes de conectar con Solace...")
    time.sleep(10)  # Tiempo para estabilizar la conexión OPC UA

    solace_service = connection_manager.connect_solace()
    if solace_service is None:
        logger.error("Error al conectar con Solace. Terminando ejecución.")
        return

    solace_publisher = create_publisher(solace_service)
    if solace_publisher is None:
        logger.error("Error al inicializar publicador de Solace. Terminando ejecución.")
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        for ctrlx in config["opcua"]["controllers"]:
            ctrlx_name = ctrlx["name"]
            opcua_client = connection_manager.get_opcua_client(ctrlx_name)
            if not opcua_client:
                logger.error(f"No se pudo obtener cliente OPC UA para {ctrlx_name}. Omitiendo...")
                continue

            nodes = ctrlx["nodes"]
            for node in nodes:
                executor.submit(extract_sensor_data, opcua_client, solace_publisher, node)

    while True:
        time.sleep(1)  # Mantiene vivo el hilo principal

if __name__ == "__main__":
    extract_data()