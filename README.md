# Real-Time Food Delivery and Order Tracking

A demo data pipeline built using Apache Airflow 3.3.1, Python, and Docker Compose.

## Features

- FileSensor waits for the orders CSV file.
- Validates incoming order data.
- Uses TaskGroups for order processing and delivery tracking.
- Uses branching to select the delivery-tracking path.
- Configures task retries and retry delays.
- Generates a JSON order summary report.
- Logs DAG success and failure callbacks.

## Technology Stack

- Apache Airflow 3.3.1
- Python
- Docker Compose
- CSV and JSON

## Workflow

1. Wait for orders.csv.
2. Validate order data.
3. Process valid orders.
4. Choose the delivery-tracking path.
5. Update delivery statuses.
6. Generate the order summary JSON report.

## Run Locally

1. Install Docker Desktop.
2. Clone this repository.
3. Open the project folder in PowerShell.
4. Run:

   ```powershell
   docker compose up -d
````

5. Open the Airflow UI using the port configured in docker-compose.yaml.
6. Trigger the food_delivery_pipeline DAG.

## Useful Commands

List DAGs:

```powershell
docker compose exec airflow-scheduler airflow dags list
```

Check import errors:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

List tasks:

```powershell
docker compose exec airflow-scheduler airflow tasks list food_delivery_pipeline
```

## Report

The pipeline generates `order_summary.json` inside the Airflow container under `/opt/airflow/reports/`.

## Notes

This is a learning/demo project. Do not upload passwords, API keys, or real customer data.
