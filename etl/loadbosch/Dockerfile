# Use Python 3.10 as the base image
FROM python:3.10.11-slim

# Expose Oracle DB port (1521) for database communication
EXPOSE 1521:1521
EXPOSE 1883:1883

# Set environment variables for Poetry
ENV POETRY_HOME="/root/.poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl netcat-traditional net-tools && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


# Set the working directory inside the container
WORKDIR /app

# Copy Poetry configuration files
COPY pyproject.toml poetry.lock ./

# Install project dependencies using Poetry
RUN poetry install --no-root

# Copy the entire source code into the container
COPY . .

# Default command to run the loading script
CMD ["poetry", "run", "python", "load.py"]
