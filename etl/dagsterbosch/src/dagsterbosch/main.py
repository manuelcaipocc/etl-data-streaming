from dagster import Definitions

from dagsterbosch.jobs import dummy_job, test_all_connections
from dagsterbosch.resources.connection_resources import (
    connection_manager_resource
)

defs = Definitions(
    jobs=[
        dummy_job,
        test_all_connections  # <- nuevo job agregado aquí
    ],
    resources={
        "cm": connection_manager_resource
    }
)
