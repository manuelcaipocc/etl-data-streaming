import psycopg2
import time
import threading
import logging
import yaml
from pathlib import Path
from datetime import datetime
import solace.messaging.messaging_service as messaging_service
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.resources.topic_subscription import TopicSubscription
from utils import utils
from opcua import Client, ua

# Configure logging
logger = logging.getLogger(__name__)

class ConnectionManager:
    """Maneja conexiones eficientes a múltiples servicios: OPC UA, Solace y PostgreSQL."""

    _instance = None  # Singleton pattern para una sola instancia de la clase

    def __new__(cls):
        """Implementa Singleton para asegurar una única instancia del ConnectionManager."""
        if cls._instance is None:
            cls._instance = super(ConnectionManager, cls).__new__(cls)
            cls._instance.opcua_clients = {}  # Diccionario de conexiones OPC UA
        return cls._instance

    def connect_opcua(self):
        """Establece conexiones persistentes con múltiples servidores OPC UA."""
        config = utils.load_config()
        controllers = config["opcua"]["controllers"]

        for ctrlx in controllers:
            ctrlx_name = ctrlx["name"]
            server_url = ctrlx["server_url_client"]
            username = ctrlx.get("username")
            password = ctrlx.get("password")
            timeout = ctrlx.get("timeout", 10)
            retries = ctrlx.get("retries", 5)
            retry_delay = ctrlx.get("retry_delay", 4)

            if ctrlx_name in self.opcua_clients:
                logger.info(f"Ya existe una conexión activa para {ctrlx_name}, omitiendo conexión.")
                continue  # Si ya existe la conexión, la reutilizamos

            logger.info(f"Intentando conectar a OPC UA ({ctrlx_name}) en {server_url}")

            for attempt in range(1, retries + 1):
                try:
                    client = Client(server_url)
                    client.session_timeout = timeout * 1000

                    if username and password:
                        client.set_user(username)
                        client.set_password(password)

                    logger.info(f"Conectando a OPC UA ({ctrlx_name}) - Intento {attempt}/{retries}")
                    client.connect()
                    self.opcua_clients[ctrlx_name] = client
                    logger.info(f"Conexión exitosa a OPC UA ({ctrlx_name})")
                    break

                except Exception as e:
                    logger.error(f"Error conectando a OPC UA ({ctrlx_name}) - Intento {attempt}: {e}")
                    if attempt < retries:
                        logger.info(f"Reintentando en {retry_delay} segundos...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"No se pudo conectar a OPC UA ({ctrlx_name}) después de varios intentos.")

    def get_opcua_client(self, ctrlx_name):
        """Devuelve el cliente OPC UA de un `CtrlX` si está activo, o reintenta conexión si es necesario."""
        client = self.opcua_clients.get(ctrlx_name)

        if client:
            try:
                client.get_node("i=85").get_value()  # Comprobación básica de conexión
                return client
            except Exception:
                logger.warning(f"La conexión OPC UA para {ctrlx_name} se ha perdido. Intentando reconectar...")
                self.connect_opcua()  # Intentar reconectar
                return self.opcua_clients.get(ctrlx_name, None)
        
        logger.error(f"No hay conexión OPC UA disponible para {ctrlx_name}.")
        return None


    @staticmethod
    def connect_solace():
        """Establishes connection with Solace PubSub+ and handles retries with a delay"""
        config=utils.load_config()
        solace_config = config["solace"]
        broker_props = {
            "solace.messaging.transport.host": f"tcp://{solace_config['host']}:{solace_config['port']}",
            "solace.messaging.service.vpn-name": solace_config["vpn"],
            "solace.messaging.authentication.scheme.basic.username": solace_config["username"],
            "solace.messaging.authentication.scheme.basic.password": solace_config["password"]
        }

        attempt = 0
        max_retries = 10
        delay_between_retries = 10  # Espera de 3 segundos antes de reintentar

        while attempt < max_retries:
            try:
                messaging_service_instance = messaging_service.MessagingService.builder() \
                    .from_properties(broker_props) \
                    .build()

                messaging_service_instance.connect()
                logger.info("Successfully connected to Solace PubSub+")
                return messaging_service_instance

            except Exception as e:
                attempt += 1
                logger.error(f"Error connecting to Solace (Attempt {attempt}/{max_retries}): {e}")
                logger.error(f" Error with keys {solace_config}")
                if attempt < max_retries:
                    logger.info(f"Waiting {delay_between_retries} seconds before retrying...")
                    time.sleep(delay_between_retries)

        logger.error("Failed to connect to Solace after multiple attempts.")
        return None
    
    


    @staticmethod
    def connect_postgres(retries=5, delay=5):
        """Establishes a connection to PostgreSQL using the configured schema and table."""
        
        config = utils.load_config()
        postgres_config = config.get("postgres", {})
        POSTGRES_HOST = postgres_config.get("host", "localhost")
        POSTGRES_USER = postgres_config.get("user", "user")
        POSTGRES_PASSWORD = postgres_config.get("password", "password")
        POSTGRES_DB = postgres_config.get("db", "postgres")
        POSTGRES_PORT = postgres_config.get("port", 5432)
        POSTGRES_SCHEMA = postgres_config.get("schema", "public")  # Default to "public" if not defined
        POSTGRES_TABLE = postgres_config.get("table", "data")
        DB_WRITE_INTERVAL = postgres_config.get("write_interval", 1)  # Default value 1 second
        
        attempt = 0
        while attempt < retries:
            try:
                conn = psycopg2.connect(
                    host=POSTGRES_HOST,
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    dbname=POSTGRES_DB,
                    port=POSTGRES_PORT,
                    options=f'-c search_path={POSTGRES_SCHEMA}'  # Asegura que el esquema sandbox sea usado
                )
                logger.info(f"Successful connection to PostgreSQL using schema {POSTGRES_SCHEMA}.")
                return conn
            except psycopg2.DatabaseError as e:
                logger.error(f"PostgreSQL connection failure (Attempt {attempt+1}/{retries}): {e}")
                time.sleep(delay * (2 ** attempt))
                attempt += 1
        logger.error("Failed to connect to PostgreSQL after multiple attempts.")
        return None
    
