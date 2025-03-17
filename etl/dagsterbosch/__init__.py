from dagster import Definitions
from dagsterbosch.jobs.etl_jobs import etl_job
from dagsterbosch.resources.database_resource import oracle_connection
from dagsterbosch.resources.docker_resource import docker_client
from dagsterbosch.sensors.ctrlx_file_sensor import ctrlx_file_sensor
from dagsterbosch.sensors.ctrlx_db_sensor import ctrlx_db_sensor
from dagsterbosch.sensors.docker_sensor import etl_container_monitor

defs = Definitions(
    jobs=[etl_job],
    resources={
        "oracle_connection": oracle_connection,
        "docker_client": docker_client
    },
    sensors=[ctrlx_file_sensor, ctrlx_db_sensor, etl_container_monitor]
)
