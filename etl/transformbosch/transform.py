import json
import logging
import yaml
import time
import solace.messaging.messaging_service as messaging_service
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.direct_message_receiver import DirectMessageReceiver
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisher
from solace.messaging.receiver.message_receiver import MessageHandler
from pathlib import Path
from utils import utils
from ConnectionManager import ConnectionManager

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
config = utils.load_config()

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
    
def publish_to_solace(publisher, topic_name, payload):
    """Processes received data, transforms it, and publishes it to another topic"""
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

class SolaceMessageHandler(MessageHandler):
    def __init__(self, solace_publisher, config):
        """Processes received data, transforms it only if BrowseName exists in YAML."""
        super().__init__()
        self.solace_publisher = solace_publisher
        self.config = config
        self.transform_config = self.load_yaml_config("format.yaml")

    def load_yaml_config(self, filepath):
        """Loads the YAML file with transformation configurations"""
        try:
            with open(filepath, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            logging.error(f"Error loading YAML file: {e}")
            return {"transformations": []}

    def transform_value(self, value, data_type, factor):
        """Converts the data type and applies the transformation factor""" 
        try:
            if data_type == "Float":
                value = float(value) * factor
            elif data_type == "Int32":
                value = int(float(value)) * factor
            return value
        except ValueError:
            logging.warning(f"Error converting value {value} to {data_type}")
            return value  

    def find_transformation(self, browse_name):
        """Finds the transformation for a BrowseName in the YAML""" 
        for t in self.transform_config.get("transformations", []):
            if t.get("BrowseName") == browse_name:
                return t
        return None   

    def on_message(self, message):
        try:
            raw_data = json.loads(message.get_payload_as_string())
            
            if not isinstance(raw_data, dict) or "BrowseName" not in raw_data:
                logger.warning("Received malformed message, skipping transformation.")
                return

            browse_name = raw_data["BrowseName"]
            transformation = self.find_transformation(browse_name)
            
            if transformation:
                transformed_value = self.transform_value(
                    raw_data["Value"], 
                    transformation.get("DataType", "Float"), 
                    transformation.get("Factor", 1.0)
                )
            else:
                transformed_value = raw_data["Value"]
            
            transformed_data = {
                "CtrlX_Name": raw_data["CtrlX_Name"],
                "Site": raw_data["Site"],
                "NamespaceIndex": raw_data["NamespaceIndex"],
                "RouteName": raw_data["RouteName"],
                "BrowseName": browse_name,
                "Value": transformed_value,
                "DataType": raw_data["DataType"],
                "processed": True,
                "Timestamp": raw_data["Timestamp"],
                "is_run_status": raw_data["is_run_status"]
            }

            publish_to_solace(self.solace_publisher, config["solace"]["topics"]["transformed_data"][0], transformed_data)
        except Exception as e:
            logger.error(f"Error processing Solace message: {e}")
                    
if __name__ == "__main__":
    solace_service = ConnectionManager.connect_solace()

    if solace_service is None:
        logger.error("Failed to connect to Solace. Exiting program.")
        exit(1)

    solace_publisher = create_publisher(solace_service)
    if solace_publisher is None:
        logger.error("Failed to initialize the publisher. Exiting program.")
        exit(1)

    topic_subscription = [TopicSubscription.of(topic) for topic in config["solace"]["topics"]["raw_data"]]

    solace_subscriber = solace_service.create_direct_message_receiver_builder() \
        .with_subscriptions(topic_subscription) \
        .build()

    solace_subscriber.start()
    solace_subscriber.receive_async(SolaceMessageHandler(solace_publisher=solace_publisher, config=config))
    logger.info("Listening for messages on raw_data...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Closing connections...")
        solace_publisher.terminate()
        solace_subscriber.terminate()
        solace_service.disconnect()