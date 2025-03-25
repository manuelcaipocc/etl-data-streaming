from dagster import job, op

@op
def hola_op(context):
    context.log.info("Hola")  # ✅ se ejecuta SOLO cuando se corre el job

@job
def dummy_job():
    hola_op()
