import docker
from dagster import job, op, get_dagster_logger

def run_extract_container():
    client = docker.from_env()
    logger = get_dagster_logger()
    container_name = "etl-extract"

    logger.info(f"Verificando estado del contenedor {container_name}...")
    try:
        container = client.containers.get(container_name)
        
        # Si el contenedor no está corriendo, lo inicia
        if container.status != "running":
            logger.info(f"Iniciando contenedor {container_name}...")
            container.restart()
        
        # 🔹 Ejecutar `extract.py` dentro del contenedor con exec_run()
        logger.info(f"Ejecutando extract.py en {container_name}...")
        exec_command = container.exec_run("poetry run python /app/extract.py", detach=True)
        logger.info(exec_command.output)

    except Exception as e:
        logger.error(f"Error al iniciar {container_name}: {e}")
        raise

@op
def start_extract():
    """Dagster inicia el contenedor `etl-extract` y ejecuta `extract.py` dentro."""
    run_extract_container()

@job
def extract_job():
    """Job de Dagster que inicia `etl-extract` y ejecuta `extract.py`."""
    start_extract()
