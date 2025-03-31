# Use Python 3.10.11 as the base image
FROM python:3.10.11-slim

EXPOSE 3000:3000

# Set environment variables for Poetry
ENV POETRY_HOME="/root/.poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Instala dependencias de sistema necesarias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl build-essential netcat-traditional net-tools docker.io docker-compose && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Establece directorio de trabajo
WORKDIR /app/dagsterbosch

# Instala Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

COPY dagsterbosch/pyproject.toml dagsterbosch/poetry.lock ./


# Install dependencies using Poetry
RUN poetry install --no-root

# Copiar el código restante
COPY dagsterbosch/ ./

# Start Dagster Webserver
CMD ["poetry", "run", "dagster-webserver", "--host", "0.0.0.0", "--port", "3000"]
