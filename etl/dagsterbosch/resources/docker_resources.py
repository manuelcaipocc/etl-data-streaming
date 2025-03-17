import docker
from dagster import resource, Definitions

@resource
def docker_client():
    """Crea un cliente para comunicarse con los contenedores del ETL."""
    return docker.from_env()

# 🔹 Registrar el recurso en Dagster
defs = Definitions(resources={"docker_client": docker_client})
