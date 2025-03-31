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


# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carga de configuración
global_config = utils.load_config()
last_values = {}
initial_publishes = {}


def create_publisher(service):
    """Crea un publicador de mensajes para Solace"""
    if service is None or not service.is_connected:
        logger.error("No hay conexión activa con Solace. No se puede crear el publicador.")
        return None
    try:
        publisher = service.create_direct_message_publisher_builder().build()
        publisher.start()
        logger.info("Publicador de Solace inicializado correctamente.")
        return publisher
    except Exception as e:
        logger.error(f"Error creando publicador de Solace: {e}")
        return None


def publish_to_solace(publisher, topic_name, payload):
    """Publica un mensaje en Solace"""
    if publisher is None or not publisher.is_ready():
        logger.error("No hay conexión activa con el publicador de Solace. No se puede publicar.")
        return
    try:
        topic = Topic.of(topic_name)
        message = json.dumps(payload)
        publisher.publish(message, topic)
        logger.info(f"Publicado en Solace ({topic_name}): {payload}")
    except Exception as e:
        logger.error(f"Error enviando mensaje a Solace: {e}")


def extract_sensor_data(opcua_client, solace_publisher, node, ctrlx_name, site):
    """Extrae datos de sensores OPC UA y publica en Solace según las reglas definidas."""
    browse_name = node["BrowseName"]
    namespace_index = node.get("NamespaceIndex", 2)
    route_name = node["RouteName"]
    update_interval = node.get("update_interval", 1000) / 1000  # Conversión de ms a s
    variation_threshold = node.get("variation_threshold", 0.05)
    is_run_status = node.get("is_run_status", False)
    table_destiny = node.get("table","1M")
    
    node_id = f"ns={namespace_index};s={route_name}"
    last_values[node_id] = None
    initial_publishes[node_id] = 0  # Contador de publicaciones iniciales obligatorias
    last_publish_time = time.time()

    time.sleep(5)  # Esperar 5 segundos antes de comenzar la extracción de datos

    while True:
        try:
            opcua_node = opcua_client.get_node(node_id)
            value = opcua_node.get_value()
            timestamp = datetime.utcnow().isoformat() + "Z"

            should_publish = False
            now=time.time()
            
            if initial_publishes[node_id] < 10:
                should_publish = True
                initial_publishes[node_id] += 1
                last_publish_time = now
            else:
                last_value = last_values[node_id]
                if is_run_status:  # Booleano: Publicar solo si hay un cambio
                    if last_value is None or value != last_value:
                        should_publish = True
                        last_publish_time = now
                else:  # Numérico: Publicar si el cambio es mayor al umbral
                    if last_value is not None:
                        delta = abs(value - last_value) / (abs(last_value) + 1e-9)
                        if delta >= variation_threshold:
                            should_publish = True
                            last_publish_time = now
                            
                # New condition: publish if 10 minutes have passed without changes
                if (now - last_publish_time) >= 600:
                    logger.info(f"10 minutes without changes in {browse_name} - publishing current value.")
                    should_publish = True
                    last_publish_time = now
                
            if should_publish:
                last_values[node_id] = value
                variable_data = {
                    "CtrlX_Name": ctrlx_name,
                    "Site": site,
                    "NamespaceIndex": namespace_index,
                    "RouteName": route_name,
                    "BrowseName": browse_name,
                    "Value": value,
                    "DataType": "Boolean" if is_run_status else "Float",
                    "processed": False,
                    "Timestamp": timestamp,
                    "is_run_status": is_run_status,
                    "table_storage": table_destiny
                }
                publish_to_solace(solace_publisher, global_config["solace"]["topics"]["raw_data"][0], variable_data)

        except Exception as e:
            logger.error(f"Error extrayendo {browse_name} ({route_name}): {e}")

        time.sleep(update_interval)


def extract_data():
    """Inicializa conexiones y lanza hilos para la extracción de datos."""
    connection_manager = ConnectionManager()
    connection_manager.connect_opcua()

    logger.info("Esperando 10 segundos antes de conectar a Solace...")
    time.sleep(20)

    solace_service = connection_manager.connect_solace()
    if not solace_service:
        logger.error("Error conectando a Solace. Terminando ejecución.")
        return

    solace_publisher = create_publisher(solace_service)
    if not solace_publisher:
        logger.error("Error inicializando publicador de Solace. Terminando ejecución.")
        return

    with ThreadPoolExecutor(max_workers=10) as executor:
        for ctrlx in global_config["opcua"]["controllers"]:
            ctrlx_name = ctrlx["name"]
            site = ctrlx.get("site", "Unknown")
            opcua_client = connection_manager.get_opcua_client(ctrlx_name)
            if not opcua_client:
                logger.error(f"No se pudo obtener el cliente OPC UA para {ctrlx_name}. Omitiendo...")
                continue

            nodes = ctrlx["nodes"]
            for node in nodes:
                executor.submit(extract_sensor_data, opcua_client, solace_publisher, node, ctrlx_name, site)

    while True:
        time.sleep(1)  # Mantiene el hilo principal activo


if __name__ == "__main__":
    extract_data()