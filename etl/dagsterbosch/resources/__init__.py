from dagster import load_assets_from_package_module
from .database_resources import oracle_connection
from .docker_resources import docker_client

# Definir todos los recursos disponibles
resources = {
    "oracle_connection": oracle_connection,
    "docker_client": docker_client,
}
