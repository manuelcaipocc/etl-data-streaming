# Use Python 3.10 as the base image
FROM python:3.10.11-slim

EXPOSE 1883:1883

# Set environment variables for Poetry
ENV POETRY_HOME="/root/.poetry"
ENV PATH="$POETRY_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    netcat-traditional \
    net-tools \
    iproute2 \
    iputils-ping \
    && \
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


# Default command to run the extraction script
CMD ["poetry", "run", "python", "extract.py"]
