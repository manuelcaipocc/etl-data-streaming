import logging
import random
import time
import threading
from datetime import datetime, timezone
from opcua import Server, ua
from utils import utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def update_node_value(node_data):
    """Updates the value of a node based on its type and update interval."""
    while True:
        opcua_node = node_data["node"]
        is_run_status = node_data["is_run_status"]
        update_interval = node_data["update_interval"]
        last_toggle_time = node_data["last_toggle_time"]
        variation_simulation = node_data["variation_simulation"]
        simulation_value = node_data["simulation_value"]
        toggle_interval = update_interval * variation_simulation  # Defines toggle frequency for boolean values

        current_value = opcua_node.get_value()
        new_value = current_value

        if is_run_status:
            if time.time() - last_toggle_time >= toggle_interval:
                new_value = not current_value  # Toggle boolean value
                node_data["last_toggle_time"] = time.time()
        else:
            min_value = simulation_value * (1 - variation_simulation)
            max_value = simulation_value * (1 + variation_simulation)
            new_value = round(random.uniform(min_value, max_value), 2)

        opcua_node.set_value(new_value)
        json_output = {
            "CtrlX_Name": node_data["ctrlx_name"],
            "Site": node_data["site"],
            "NamespaceIndex": node_data.get("namespace_index", 2),
            "RouteName": node_data.get("route_name", "Unknown"),
            "BrowseName": node_data["browse_name"],
            "Value": new_value,
            "DataType": "Boolean" if is_run_status else "Float",
            "processed": False,
            "Timestamp": datetime.now(timezone.utc).isoformat()
        }
        logger.info(f"Updated node {node_data['browse_name']}: {json_output}")
        time.sleep(update_interval)  # Wait for the next update

def start_opcua_server(controller_config):
    """Initializes and runs an OPC UA server for a specific CtrlX."""

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
    threads = []
    ctrlx_name = controller_config["name"]
    site = controller_config.get("site", "Unknown")

    for node in controller_config["nodes"]:
        node_id_str = node["NodeId"]
        browse_name = node["BrowseName"]
        update_interval = node.get("update_interval", 1000) / 1000  # Convert ms to seconds
        is_run_status = node.get("is_run_status", False)
        initial_value = node.get("simulation_value", 0.0)
        variation_simulation = node.get("variation_simulation", 0.05)

        node_id_obj = ua.NodeId.from_string(node_id_str)
        opcua_type = ua.VariantType.Boolean if is_run_status else ua.VariantType.Float
        opcua_node = ctrlx_nodes.add_variable(node_id_obj, browse_name, initial_value, opcua_type)
        opcua_node.set_writable()

        node_data = {
            "node": opcua_node,
            "browse_name": browse_name,
            "update_interval": update_interval,
            "is_run_status": is_run_status,
            "variation_simulation": variation_simulation,
            "simulation_value": initial_value,
            "last_toggle_time": time.time(),
            "namespace_index": node.get("NamespaceIndex", 2),
            "route_name": node.get("RouteName", "Unknown"),
            "ctrlx_name": ctrlx_name,
            "site": site
        }

        nodes_dict[node_id_str] = node_data
        opcua_node.set_value(initial_value)
        logger.info(f"[{controller_config['name']}] Node added: {browse_name} (NodeId: {node_id_str}, Update Interval: {update_interval}s, Initial Value: {initial_value})")

        # Start a separate thread for each node
        thread = threading.Thread(target=update_node_value, args=(node_data,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    if not nodes_dict:
        logger.info(f"[{controller_config['name']}] No nodes to simulate. Server will not start.")
        return

    try:
        server.start()
        logger.info(f"[{controller_config['name']}] OPC UA Server started at {server_url}")
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        logger.warning(f"[{controller_config['name']}] Shutting down the server...")
    finally:
        server.stop()
        logger.info(f"[{controller_config['name']}] Server shut down successfully.")

if __name__ == "__main__":
    config = utils.load_config()

    threads = []
    servers_to_simulate = False

    for controller in config["opcua"]["controllers"]:
        if "server_url_server" in controller and controller["server_url_server"]:
            servers_to_simulate = True
            thread = threading.Thread(target=start_opcua_server, args=(controller,))
            thread.start()
            threads.append(thread)
        else:
            logger.info(f"Skipping {controller['name']} (No 'server_url_server' found).")

    if not servers_to_simulate:
        logger.info("No necessary OPC UA server simulation. Exiting gracefully.")
        exit(0)

    for thread in threads:
        thread.join()
