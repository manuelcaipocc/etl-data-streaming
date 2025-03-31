from dagster import ScheduleDefinition
from dagsterbosch.jobs.monitor_resources_job import monitor_all_connections

connections_schedule = ScheduleDefinition(
    job=monitor_all_connections,
    cron_schedule="*/5 * * * *",  # cada 5 minutos
)
