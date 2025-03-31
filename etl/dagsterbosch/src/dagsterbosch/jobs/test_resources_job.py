from dagster import job, op
from utils import utils
from dagsterbosch.resources.connection_resources import (
    connection_manager_resource)


@op(required_resource_keys={"cm"})
def test_postgres_op(context):
    conn = context.resources.cm.conn
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    context.log.info(f"PostgreSQL respondió: {result}")
    context.resources.cm.disconnect_postgres()

@op(required_resource_keys={"cm"})
def test_solace_op(context):
    solace_service = context.resources.cm.messaging_service_instance
    try:
        publisher = solace_service.create_direct_message_publisher_builder().build()
        context.log.info("Conexión a Solace verificada exitosamente al crear un publisher.")
    except Exception as e:
        context.log.error(f"No se pudo verificar la conexión a Solace: {e}")
    finally:
        try:
            publisher.terminate()
            context.log.info("Publisher de Solace cerrado.")
        except Exception as e:
            context.log.warning(f"No se pudo cerrar el publisher: {e}")

@op(required_resource_keys={"cm"})
def test_opcua_op(context):
    cm = context.resources.cm # es tu ConnectionManager
    config = utils.load_config()
    controllers = config["opcua"]["controllers"]

    for ctrlx in controllers:
        name = ctrlx["name"]
        client = cm.get_opcua_client(name)

        if client:
            context.log.info(f"Conexión a OPC UA ({name}) verificada correctamente.")
            cm.disconnect_opcua(name)
            context.log.info(f"Conexión a OPC UA ({name}) cerrada.")
        else:
            context.log.warning(f"No se pudo obtener cliente OPC UA para {name}.")


@job(resource_defs={"cm": connection_manager_resource})
def test_all_connections():
    test_postgres_op()
    test_solace_op()
    test_opcua_op()
