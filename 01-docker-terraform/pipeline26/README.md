# example command to ingest data

```
uv run python ingest_data.py \
  --pg-user=dateng26 \
  --pg-pass=dateng26 \
  --pg-host=localhost \
  --pg-port=5432 \
  --pg-db=dateng \
  --target-table=yellow_taxi_trips_2021_1 \
  --year=2021 \
  --month=1 \
  --chunksize=100000


uv run python ingest_data.py \
  --target-table=yellow_taxi_trips \
  --year=2021 \
  --month=1 \
  --chunksize=100000


docker run -it --rm --network=dateng26 taxi_ingest:v001 \
  --target-table=yellow_taxi_trips_2021_docker \
  --pg-host=postgres-dateng \
  --year=2021 \
  --month=1 \
  --chunksize=100000

```
