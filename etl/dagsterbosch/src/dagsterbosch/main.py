from dagster import Definitions
from dagsterbosch.schedules.schedule_monitor import connections_schedule
from dagsterbosch.jobs import dummy_job, test_all_connections, monitor_all_connections
from dagsterbosch.resources.connection_resources import (
    connection_manager_resource
)

defs = Definitions(
    jobs=[
        dummy_job,
        test_all_connections,
        monitor_all_connections
    ],
    schedules=[connections_schedule],
    resources={
        "cm": connection_manager_resource
    }
)
