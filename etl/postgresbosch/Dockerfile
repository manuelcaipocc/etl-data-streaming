FROM postgres:16

# Instalar pg_cron
RUN apt-get update && \
    apt-get install -y postgresql-16-cron && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# pg_cron necesita estar en shared_preload_libraries, lo activamos con el CMD en docker-compose
