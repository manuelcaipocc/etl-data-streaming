import logging
import random
import time
import yaml
import threading
from datetime import datetime, timezone
from opcua import Server, ua
from pathlib import Path
from utils import utils
# from etl.utils import utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def start_opcua_server(controller_config):
    """Initializes and runs an OPC UA server for a specific CtrlX."""

    # Verify if the controller has a server URL defined
    if "server_url_server" not in controller_config or not controller_config["server_url_server"]:
        logger.info(f"Skipping simulation for {controller_config['name']} (no server_url_server found).")
        return

    server = Server()
    server_url = controller_config["server_url_server"]
    server.set_endpoint(server_url)
    server.set_server_name(f"Bosch CtrlX Simulation Server - {controller_config['name']}")

    logger.info(f"Starting OPC UA Server for {controller_config['name']} at {server_url}...")

    uri = str(controller_config["namespace"])
    idx = server.register_namespace(uri)

    objects_node = server.get_objects_node()
    ctrlx_nodes = objects_node.add_object(ua.NodeId(f"{controller_config['name']}_Nodes", idx), f"{controller_config['name']}_Nodes")

    nodes_dict = {}

    for node in controller_config["nodes"]:
        node_id_str = node["NodeId"]
        browse_name = node["BrowseName"]
        node_update_interval = node.get("update_interval", 1000) / 1000  # Convert ms to seconds
        run_status = node.get("run_status", False)
        initial_value = node.get("simulation_value", 0.0)
        variation_threshold = node.get("variation_simulation", 0.05)


        node_id_obj = ua.NodeId.from_string(node_id_str)
        initial_value = 0.0

        opcua_type = ua.VariantType.Float
        opcua_node = ctrlx_nodes.add_variable(node_id_obj, browse_name, initial_value, opcua_type)
        opcua_node.set_writable()

        variation = [0.0, 10.0]
        nodes_dict[node_id_str] = {
            "node": opcua_node,
            "browse_name": browse_name,
            "variation": variation,
            "update_interval": node_update_interval  # Store frequency per node
        }

        logger.info(f"[{controller_config['name']}] Node added: {browse_name} (NodeId: {node_id_str}, Update Interval: {node_update_interval}s)")

    try:
        server.start()
        logger.info(f"[{controller_config['name']}] OPC UA Server started at {server_url}")
    except Exception as e:
        logger.error(f"Error starting the OPC UA server for {controller_config['name']}: {e}")
        return

    # Update loop with individual frequencies per node
    try:
        while True:
            timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
            batch_data = {"Timestamp": timestamp, "Data": []}

            for node_id_str, node_data in nodes_dict.items():
                opcua_node = node_data["node"]
                browse_name = node_data["browse_name"]
                variation = node_data["variation"]
                node_update_interval = node_data["update_interval"]

                current_value = opcua_node.get_value()
                new_value = round(current_value + random.uniform(variation[0], variation[1]), 2)
                opcua_node.set_value(new_value)

                data_entry = {
                    "NodeId": node_id_str,
                    "BrowseName": browse_name,
                    "DisplayName": browse_name,
                    "Value": new_value,
                    "DataType": "Float"
                }
                batch_data["Data"].append(data_entry)

                # Control individual frequency per node
                time.sleep(node_update_interval)

            logger.info(f"[{controller_config['name']}] Updating values: {batch_data}")

    except KeyboardInterrupt:
        logger.warning(f"[{controller_config['name']}] Shutting down the server...")
    finally:
        server.stop()
        logger.info(f"[{controller_config['name']}] Server shut down successfully.")


def stop_simulation():
    """Function to stop the simulation with keys."""
    global running
    running = False
    logger.warning("Simulation stopping... Press CTRL+C if it does not stop automatically.")


if __name__ == "__main__":
    config = utils.load_config()

    # Listen for the "q" key to stop the simulation
    # keyboard.add_hotkey("q", stop_simulation)

    threads = []
    servers_to_simulate = False 
    
    for controller in config["opcua"]["controllers"]:
        # Only start servers that have a defined OPC UA server URL
        if "server_url_server" in controller and controller["server_url_server"]:
            servers_to_simulate = True 
            thread = threading.Thread(target=start_opcua_server, args=(controller,))
            thread.start()
            threads.append(thread)
        else:
            logger.info(f"Skipping {controller['name']} (No 'server_url_server' found).")
    
    if not servers_to_simulate:
        logger.info("Simulation is not necessary")
            
    for thread in threads:
        thread.join()