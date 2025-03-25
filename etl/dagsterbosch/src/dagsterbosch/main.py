from dagster import Definitions

from dagsterbosch.jobs import dummy_job, test_all_connections
from dagsterbosch.resources.connection_resources import (
    postgres_resource,
    solace_resource,
    opcua_resource
)

defs = Definitions(
    jobs=[
        dummy_job,
        test_all_connections  # <- nuevo job agregado aquí
    ],
    resources={
        "postgres": postgres_resource,
        "solace": solace_resource,
        "opcua": opcua_resource
    }
)
