from dagster import job, op
from utils import utils
from dagsterbosch.resources.connection_resources import connection_manager_resource

@op(required_resource_keys={"cm"})
def monitor_postgres_op(context):
    try:
        conn = context.resources.cm.conn
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        context.log.info(f"PostgreSQL respondió: {result}")
        context.resources.cm.disconnect_postgres()
    except Exception as e:
        context.log.error(f"Error en PostgreSQL: {e}")
        utils.restart_container("etl-extract", context)

@op(required_resource_keys={"cm"})
def monitor_solace_op(context):
    try:
        solace_service = context.resources.cm.messaging_service_instance
        publisher = solace_service.create_direct_message_publisher_builder().build()
        context.log.info("Conexión a Solace verificada.")
        publisher.terminate()
    except Exception as e:
        context.log.error(f"Error en Solace: {e}")
        utils.restart_container("etl-extract", context)

@op(required_resource_keys={"cm"})
def monitor_opcua_op(context):
    cm = context.resources.cm
    config = utils.load_config()
    controllers = config["opcua"]["controllers"]

    for ctrlx in controllers:
        name = ctrlx["name"]
        try:
            client = cm.get_opcua_client(name)
            if client:
                context.log.info(f"Conexión OPC UA ({name}) verificada.")
                cm.disconnect_opcua(name)
            else:
                raise Exception("No se pudo obtener cliente")
        except Exception as e:
            context.log.error(f"Error en OPC UA ({name}): {e}")
            utils.restart_container("etl-extract", context)

@job(resource_defs={"cm": connection_manager_resource})
def monitor_all_connections():
    monitor_postgres_op()
    monitor_solace_op()
    monitor_opcua_op()
