import os
import json
import logging
import threading
import time
import psycopg2
import solace.messaging.messaging_service as messaging_service
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.receiver.message_receiver import MessageHandler
from pathlib import Path
import re
from utils import utils
from ConnectionManager import ConnectionManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

config = utils.load_config()
postgres_config = config.get("postgres", {})
DB_WRITE_INTERVAL = postgres_config.get("write_interval", 1)  # Default value 1 second
DB_BATCH_SIZE = postgres_config.get("batch_size", 100)  # Minimum batch size before writing
BACKUP_DIR = "backup_data"
INITIAL_UPLOAD_DURATION=postgres_config.get("initial_upload_duration", 20)

data_buffer = []
buffer_lock = threading.Lock()

os.makedirs(BACKUP_DIR, exist_ok=True)

def get_postgres_connection():
    while True:
        try:
            manager = ConnectionManager()
            conn = manager.connect_postgres()
            if conn:
                logger.info("Connected to PostgreSQL successfully.")
                return conn
        except psycopg2.OperationalError as e:
            logger.error(f"PostgreSQL connection failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)  


def extract_code_prefix(browse_name):
    """Extracts the first six letters from `BrowseName`, ensuring meaningful representation."""
    return browse_name[:6].upper() if browse_name else "UNK"  # Use 'UNK' if the name is empty

def generate_code(browse_name):
    """Generates a unique code based on a six-letter prefix from `BrowseName`."""
    return extract_code_prefix(browse_name)

def validate_record(record):
    required_keys = {"NamespaceIndex", "RouteName", "BrowseName", "Value", "DataType", "Timestamp", "CtrlX_Name", "Site", "is_run_status"}
    return all(key in record for key in required_keys)

def save_to_backup(data_batch):
    if data_batch:
        timestamp_start = data_batch[0]["Timestamp"]
        timestamp_end = data_batch[-1]["Timestamp"]
        backup_filename = f"{BACKUP_DIR}/backup_{timestamp_start}_{timestamp_end}.json"
        with open(backup_filename, "w") as backup_file:
            json.dump(data_batch, backup_file)
        logger.info(f"Saved {len(data_batch)} records to backup: {backup_filename}")

def load_backup_files():
    backup_files = sorted(Path(BACKUP_DIR).glob("backup_*.json"))
    return backup_files

def process_backup_files(conn):
    backup_files = load_backup_files()
    for backup_file in backup_files:
        try:
            with open(backup_file, "r") as file:
                data_batch = json.load(file)
            if insert_data(conn, data_batch):
                os.remove(backup_file)
                logger.info(f"Successfully inserted backup data and deleted {backup_file}")
        except Exception as e:
            logger.error(f"Failed to process backup file {backup_file}: {e}")

def insert_data(conn, batch_data):
    if not isinstance(batch_data, list) or not all(validate_record(record) for record in batch_data):
        logger.error("Skipping insertion due to invalid data format.")
        return False
    query = """
    INSERT INTO sandbox.ctrlx_data (NamespaceIndex, RouteName, BrowseName, Value, DataType, Timestamp, Code, CtrlX_Name, Site, is_run_status,table_storage)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
    """
    try:
        with conn.cursor() as cursor:
            records = [(d["NamespaceIndex"],
                        d["RouteName"],
                        d["BrowseName"],
                        float(d["Value"]) if isinstance(d["Value"], (bool, int)) else d["Value"],
                        d["DataType"],
                        d["Timestamp"],
                        d["CtrlX_Name"]+"_"+generate_code(d["BrowseName"]),
                        d["CtrlX_Name"],
                        d["Site"],
                        d["is_run_status"],
                        d["table_storage"]
                        ) for d in batch_data]
            cursor.executemany(query, records)
            conn.commit()
            logger.info(f"Inserted {len(records)} records into PostgreSQL.")
            return True
    except psycopg2.DatabaseError as e:
        logger.error(f"Error inserting data into PostgreSQL: {e}")
        conn.rollback()
        return False

class SolaceLoadHandler(MessageHandler):
    def __init__(self):
        super().__init__()

    def on_message(self, message):
        global data_buffer
        try:
            record = json.loads(message.get_payload_as_string())
            if validate_record(record):
                with buffer_lock:
                    data_buffer.append(record)
                    logger.info(f"Record added to buffer. Current size: {len(data_buffer)}")
        except json.JSONDecodeError:
            logger.error("Error decoding JSON from payload.")
        except Exception as e:
            logger.error(f"Error processing Solace message: {e}")

def database_writer():
    global data_buffer
    conn = get_postgres_connection()
    last_write_time = time.time()
    while True:
        time.sleep(0.1)
        current_time = time.time()

        with buffer_lock:
            # Fase 1: primeros 20 segundos — subir todo lo más rápido posible
            if (current_time - start_time) <= INITIAL_UPLOAD_DURATION:
                if data_buffer:
                    try:
                        if not insert_data(conn, data_buffer):
                            save_to_backup(data_buffer)
                        else:
                            process_backup_files(conn)
                        data_buffer.clear()
                        last_write_time = current_time
                    except psycopg2.OperationalError:
                        logger.error("Lost PostgreSQL connection. Reconnecting...")
                        conn = get_postgres_connection()
                    except Exception as e:
                        logger.error(f"Unexpected error in database_writer: {e}")
            # Fase 2: comportamiento por lotes
            elif len(data_buffer) >= DB_BATCH_SIZE or (current_time - last_write_time) >= DB_WRITE_INTERVAL:
                if data_buffer:
                    try:
                        if not insert_data(conn, data_buffer):
                            save_to_backup(data_buffer)
                        else:
                            process_backup_files(conn)
                        data_buffer.clear()
                        last_write_time = current_time
                    except psycopg2.OperationalError:
                        logger.error("Lost PostgreSQL connection. Reconnecting...")
                        conn = get_postgres_connection()
                    except Exception as e:
                        logger.error(f"Unexpected error in database_writer: {e}")


if __name__ == "__main__":
    manager = ConnectionManager() 
    solace_service = manager.connect_solace()
    if solace_service is None:
        logger.error("Failed to connect to Solace. Exiting program.")
        exit(1)

    topic_subscription = [TopicSubscription.of(topic) for topic in config["solace"]["topics"]["transformed_data"]]
    solace_subscriber = solace_service.create_direct_message_receiver_builder() \
        .with_subscriptions(topic_subscription) \
        .build()

    solace_subscriber.start()
    solace_subscriber.receive_async(SolaceLoadHandler())
    
    start_time = time.time() 
    threading.Thread(target=database_writer, daemon=True).start()
    logger.info("Listening for messages on transformed_data...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Closing connections...")
        solace_subscriber.terminate()
        solace_service.disconnect()
