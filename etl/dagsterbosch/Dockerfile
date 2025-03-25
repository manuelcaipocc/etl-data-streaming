# Use Python 3.10.11 as the base image
FROM python:3.10.11-slim

EXPOSE 3000:3000

# Set environment variables for Poetry
ENV POETRY_HOME="/root/.poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential netcat-traditional net-tools && \ 
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy Poetry configuration files
COPY pyproject.toml poetry.lock ./

COPY src/ ./src/
# Install dependencies using Poetry
RUN poetry install --no-root

# Copy the application code into the container
COPY . .

# Start Dagster Webserver
CMD ["poetry", "run", "dagster-webserver", "--host", "0.0.0.0", "--port", "3000"]
