# Artemia Airflow

Apache Airflow project configured using Docker Compose.

## Setup

This project follows the official Apache Airflow Docker Compose setup guide:
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

## Quick Start

```bash
# Start services
docker-compose up -d

# Access Airflow web UI
# http://localhost:8080
```

## Project Structure

- `dags/` - Airflow DAGs
- `logs/` - Airflow logs
- `plugins/` - Custom plugins
- `config/` - Configuration files
