# ETL Project for CtrlX (Bosch Rexroth)

This application allows extracting data from any **CtrlX (Bosch Rexroth)** to a database. Currently, the data is stored in a local **PostgreSQL** database, but it can be easily migrated to a cloud service without issues.

## Table of Contents

0. [Quick Start](#0-quick-start)  
1. [Project Structure](#1-project-structure)  
2. [Prerequisites](#2-prerequisites)  
3. [Execution](#3-execution)  
4. [User and Password Verification](#4-user-and-password-verification)  
5. [Configuration File (config.yaml)](#5-configuration-file-configyaml)  
   - 5.1 [OPC UA Configuration](#51-opc-ua-configuration)  
   - 5.2 [Nodes Configuration](#52-nodes-configuration)  
   - 5.3 [Solace Configuration](#53-solace-configuration)  
   - 5.4 [PostgreSQL Configuration](#54-postgresql-configuration)  
6. [Testing OPC UA Connection](#6-testing-opc-ua-connection)  
7. [Repository](#7-repository)  
8. [Dagster Monitoring and PostgreSQL Pivoting](#8-dagster-monitoring-and-postgresql-pivoting)  
9. [PostgreSQL Pivoting](#9-postgresql-pivoting)  
10. [Contact Information](#10-contact-information)  

---

## 0. Quick Start

After step 3 (Execution), you can connect to the database by executing the following command (assuming Docker is in the system PATH):

```bash
docker exec -it postgres-db psql -U boschrexroth -d automax
```

Once connected, you can run queries such as:

```sql
SELECT * FROM sandbox.ctrlx_signals;
SELECT * FROM sandbox.ctrlx_pivot_100ms ORDER BY timestamp DESC LIMIT 10;
\dt sandbox.*;
```

To connect via Python or other services, use the following credentials:

```yaml
postgres:
  host: localhost
  port: 5432
  user: boschrexroth
  password: boschrexroth
  db: automax
  schema: sandbox
  table: ctrlx_data
```

Optionally, use the `ConnectionManager.py` class to handle the connection.

---



## 1. Project Structure


The following scheme represents the project structure:

```plaintext
etl_project/
├── ctrlxbosch/                  # Simulated CtrlX OPC UA server
│   ├── Dockerfile               # Docker container configuration
│   ├── mock_server.py           # OPC UA server simulation script
│   ├── pyproject.toml           # Poetry dependencies
│   └── poetry.lock
├── dagsterbosch/                # Dagster orchestration
│   ├── Dockerfile
│   ├── workspace.yaml           # Dagster workspace configuration
│   └── etl_jobs.py              # ETL job definitions
├── extractbosch/                # ETL: Data extraction
│   ├── Dockerfile
│   ├── extract.py               # Data extraction script
│   ├── pyproject.toml
│   └── poetry.lock
├── transformbosch/              # ETL: Data transformation
│   ├── Dockerfile
│   ├── transform.py             # Data transformation script
│   ├── pyproject.toml
│   └── poetry.lock
├── loadbosch/                   # ETL: Data loading to PostgreSQL
│   ├── Dockerfile
│   ├── load.py                  # Data loading script
│   ├── pyproject.toml
│   └── poetry.lock
├── solacebosch/                 # Solace Broker configuration
│   ├── Dockerfile
│   └── solace_config.json       # Solace broker configuration
├── oracledbbosch/               # PostgreSQL DB initialization
│   └── init.sql                 # SQL script for database schema initialization
├── data/                        # Data directories for logs and inputs
│   ├── input/                   # Input data for ETL
│   ├── logs/                    # Service logs
│   └── processed/               # Processed data outputs
├── docker-compose.yml           # Docker Compose file for orchestration
└── README.md                    # Project documentation
```
<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 2. Prerequisites

To run this project, ensure you have the following installed:

- **Developer Bosch Computer**  
  [Access to Bosch internal documentation](https://service-management.bosch.tech/sp?id=search_itsp&spa=1&t=rsc&q=bcn%20to%20internet%20client)
- **Docker**: Container orchestration

<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 3. Execution

To start the ETL pipeline, execute the following commands:

```sh
# Start Docker containers
docker-compose up -d
```
<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 4. User and Password Verification

To verify the necessary users and passwords to access and use the service on the host, check the **`docker-compose.yaml`** file.

<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 5. Configuration File (config.yaml)

The `config.yaml` file contains all the configurations required to connect the ETL system to the **CtrlX OPC UA Server**, **Solace Message Broker**, and **PostgreSQL Database**.

### 5.1 **OPC UA Configuration**
```yaml
opcua:
  server_url_server: "opc.tcp://0.0.0.0:4840"
  server_url_client: "opc.tcp://192.168.1.1:4840"
  namespace: 192.168.1.1
  username: "boschrexroth"
  password: "Boschrexroth1"
  interface: 15
  security_mode: "SignAndEncrypt"
  security_policy: "Basic256Sha256"
  private_key_path: "/certs/client-key.pem"
  timeout: 10
  update_interval: 1
  retries: 10
  retry_delay: 4
```
#### Explanation:
- **server_url_server**: The OPC UA server URL (localhost for simulation).
- **server_url_client**: The real CtrlX OPC UA URL.
- **namespace**: The namespace index for the OPC UA nodes.
- **username/password**: Authentication credentials.
- **interface**: Network interface index.
- **security_mode/security_policy**: Defines encryption settings.
- **private_key_path**: Path to the private key for secure connection.
- **timeout/update_interval/retries/retry_delay**: Network and reconnection settings.

### 5.2 **Nodes Configuration**

```yaml
nodes:
  - NodeId: "ns=2;s=plc/app/Application/sym/GVL_HMI/P_s"
    NamespaceIndex: 2
    RouteName: "plc/app/Application/sym/GVL_HMI/P_s"
    BrowseName: "P_s"
    Level: 8
    update_interval: 50               # How frequently to request updates from OPC UA (in milliseconds)
    variation_threshold: 0.01         # Threshold for detecting significant variation (1%)
    is_run_status: False              # Whether this variable represents a boolean run status
    table_storage: 100mS              # Determines the target table after pivoting (e.g., 100mS, 1S, 1M)
    publish_all_data: True            # If True, all data is published to Solace topic; otherwise, only changed values
    simulation_value: 100             # Default value when running in simulation mode
    variation_simulation: 0.06        # Simulated variation percentage (e.g., 6%)
```
#### Explanation

- **NodeId**: Unique identifier for the OPC UA node, including its namespace and path.
- **NamespaceIndex**: Index used to define the namespace in the OPC UA server.
- **RouteName**: Full internal path of the node in the PLC.
- **BrowseName**: Human-readable short name for the variable.
- **Level**: Depth or hierarchy level within the PLC structure.
- **update_interval**: How often the system should request an update from the OPC UA server (in milliseconds).
- **variation_threshold**: Minimum change required to consider the value as updated (e.g., 0.01 = 1%).
- **is_run_status**: Boolean flag indicating whether this node is a status signal (e.g., start/stop).
- **table_storage**: Frequency bucket for pivoting data (e.g., `100mS`, `1S`, `1M`), used to route values to the correct table.
- **publish_all_data**: If set to `True`, publishes every value received; otherwise, only publishes values that vary.
- **simulation_value**: Default static value for simulation purposes (e.g., if CtrlX is not connected).
- **variation_simulation**: Simulated variation range (e.g., 0.06 = ±6%) used when generating synthetic data.


### 5.3 **Solace Configuration**
```yaml
solace:
  host: solace-broker
  port: 55555
  username: admin
  password: admin
  vpn: default
  topics:
    raw_data: [opcua/data]
    transformed_data: [ctrlx/transformed]
```
#### Explanation:
- **host/port**: Address and port of the Solace broker.
- **username/password**: Credentials for Solace.
- **vpn**: Virtual Private Network (default setting).
- **topics**: Defines message topics for raw and transformed data.

### 5.4 **PostgreSQL Configuration**
```yaml
postgres:
  host: postgres-db
  port: 5432
  user: boschrexroth
  password: boschrexroth
  db: automax
  schema: sandbox
  table: ctrlx_data
  write_interval: 600
  batch_size: 10000
```
#### 5.5 Explanation:
- **host/port**: PostgreSQL database connection.
- **user/password**: Authentication credentials.
- **db/schema/table**: Defines database, schema, and table used.
- **write_interval**: Time interval (in seconds) between batch writes.
- **batch_size**: Number of records per batch insertion.

<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 6. Testing OPC UA Connection

Inside the `src/` directory, there is a Jupyter Notebook that allows testing the connection to a real **CtrlX OPC UA** server. This notebook extracts all available OPC UA nodes and stores them in a JSON file.

### 6.1 **Location of the notebook**
```plaintext
src/
├── opcua_test.ipynb   # Notebook for testing OPC UA connection
└── nodos_opcua.json   # Extracted OPC UA nodes
```
<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

## 7. Repository

The source code for this project is available in the following Bitbucket repository:

[ETL Process - CtrlX Repository](https://sourcecode.socialcoding.bosch.com/projects/SSD6TRAINING/repos/etl-ctrlx/browse)

<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>


## 8. Dagster Monitoring and PostgreSQL Pivoting

### Dagster Monitoring

Dagster automatically monitors the system's critical connections:

- **PostgreSQL**: Verifies availability and disconnects after validation.
- **Solace**: Establishes and terminates a publishing channel to validate connectivity.
- **OPC UA**: Iterates over multiple CtrlX controllers and checks if the OPC UA client can connect.

Main job:

```python
@job(resource_defs={"cm": connection_manager_resource})
def monitor_all_connections():
    monitor_postgres_op()
    monitor_solace_op()
    monitor_opcua_op()
```

This job runs periodically according to the `connections_schedule` defined in Dagster and restarts the `etl-extract` container if a failure is detected.

---

## 9 PostgreSQL Pivoting

PostgreSQL uses the **pg_cron** extension to run functions automatically at regular intervals (e.g., every 5 minutes).

Each scheduled task performs the following logic per frequency (`500mS`, `1S`, `1M`):

#### 9.1 **Refresh unique signals**:
```sql
SELECT sandbox.refresh_ctrlx_signals();
```

#### 9.2 **Initialize new columns in pivot and temporary tables**:
```sql
SELECT sandbox.initialize_columns_for_frequency('1S');
SELECT sandbox.initialize_tmp_columns_for_frequency('1S');
```

#### 9.3 **Pivot from `ctrlx_data` into temporary table**:
```sql
SELECT sandbox.pivot_ctrlx_data('1S');
```

#### 9.4 **Sync to final pivot table**:
```sql
SELECT sandbox.sync_ctrlx_pivot_table('1S');
```

####  `pg_cron` schedule:
```sql
SELECT cron.schedule(
  'pivot_1S',
  '*/5 * * * *',
  $$
  SELECT sandbox.initialize_columns_for_frequency('1S');
  SELECT sandbox.pivot_ctrlx_data('1S');
  SELECT sandbox.sync_ctrlx_pivot_table('1S');
  $$
);
```

## 10. Contact Information

For any inquiries regarding the project, please contact the main users:

| **Main User** | **Responsibilities** |
|--------------|----------------------|
| **Caipo Manuel (DC/SSD3)**          | Working Student / Developer |
| **Guedria Mohamed Amine (DC/SSD6)** | Trainer / SW Engineer |


<p style="font-size: 12px; opacity: 0.5; text-align: left;">
    <a href="#table-of-contents">⬆ Back to Table of Contents</a>
</p>

