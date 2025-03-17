
```mermaid
graph TD;
    CtrlX[ctrlx-nodes (Simulated/Real)] -->|Connects| Extract[etl-extract];
    Extract -->|Publishes data to opcua/data| Solace[solace-broker];
    Solace -->|Subscribes to opcua/data| Transform[etl-transform];
    Transform -->|Publishes transformed data to ctrlx/transformed| Solace;
    Solace -->|Subscribes to ctrlx/transformed| Load[etl-load];
    Load -->|Saves data to PostgreSQL (batch of 10,000 records)| DB[postgres-db];

    Telegraf[telegraf] -->|Publishes monitoring data| InfluxDB[influxdb];
    Grafana[grafana] -->|Queries and visualizes real-time data| InfluxDB;

    DagsterWeb[dagster-webserver] -->|Coordinates and restarts services| DagsterDaemon[dagster-daemon];
    DagsterDaemon -->|Monitors Extract execution| Extract;
    DagsterDaemon -->|Monitors Transform execution| Transform;
    DagsterDaemon -->|Monitors Load execution| Load;

    style CtrlX fill:#ffcccc,stroke:#ff0000,stroke-width:2px,color:#000,font-size:16px;
    style Extract fill:#ccffcc,stroke:#009900,stroke-width:2px,color:#000,font-size:16px;
    style Solace fill:#ccccff,stroke:#0000ff,stroke-width:2px,color:#000,font-size:16px;
    style Transform fill:#ffffcc,stroke:#ffcc00,stroke-width:2px,color:#000,font-size:16px;
    style Load fill:#ffccff,stroke:#cc00cc,stroke-width:2px,color:#000,font-size:16px;
    style DB fill:#ccffff,stroke:#00cccc,stroke-width:2px,color:#000,font-size:16px;
    style InfluxDB fill:#ff9966,stroke:#ff3300,stroke-width:2px,color:#000,font-size:16px;
    style Telegraf fill:#99ccff,stroke:#0066cc,stroke-width:2px,color:#000,font-size:16px;
    style DagsterWeb fill:#ff6699,stroke:#cc0033,stroke-width:2px,color:#000,font-size:16px;
    style DagsterDaemon fill:#9966ff,stroke:#6600cc,stroke-width:2px,color:#000,font-size:16px;
    style Grafana fill:#66ffcc,stroke:#00cc99,stroke-width:2px,color:#000,font-size:16px;

```