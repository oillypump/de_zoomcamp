# 02-worflow-orchestration

## commands

```bash
uv init --python=3.12.12 --name=workflow --no-readme
```

## challenge -1

1. Extract data from CSV files.
2. Load it into Postgres or Google Cloud (GCS + BigQuery).
3. Explore scheduling and backfilling workflows.

## docker-compose

### start services

docker compose sudah disesuaikan dengan kebutuhan Local Development Airflow.

```bash
cd 02-workflow-orchestration
docker compose run airflow-cli airflow config list
docker compose up airflow-init
docker compose up -d
```

### remove services

```bash
docker compose down --volumes --rmi local
```

### start jupyter notebook

```bash
uv add --dev jupyter
```
