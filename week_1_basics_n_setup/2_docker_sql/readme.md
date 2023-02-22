<!-- video source  -->

# source video

source video : [link](https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=8&ab_channel=DataTalksClub%E2%AC%9B)

```bash
docker run -it \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_USER="root" \
-e POSTGRES_DB="ny_taxi" \
-e TZ="Asia/Jakarta" \
-e PGTZ="Asia/Jakarta" \
-p 5432:5432 \
-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
postgres:13
```

```bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
dpage/pgadmin4
```

## docker network

```
docker run -it \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_USER="root" \
-e POSTGRES_DB="ny_taxi" \
-e TZ="Asia/Jakarta" \
-e PGTZ="Asia/Jakarta" \
-p 5432:5432 \
-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
--network=de-network \
--name de-pg-database \
postgres:13
```

```
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=de-network \
--name de-pg-admin \
dpage/pgadmin4
```

# ingest

```
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"


python ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips \
 --url=${URL}
```

## docker build

```
docker build -t taxi_ingest:v001 .
```

## execute

make sftp just type it where the directory you want to share:

```
$python3 -m http.server
```

#### wget to github:

- URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

#### wget to local sftp:

- URL="http://172.31.112.182:8000/yellow_tripdata_2021-01.csv"

```
docker run -it \
--network=de-network \
taxi_ingest:v001 \
--user=root \
--password=root \
--host=de-pg-database \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips \
--url=${URL}
```

## docker compose

actually no need to assign network on container, bcs technically.
pg admin and pgdatabase is in 1 environment.

but if it need to be access from another container so it should be attached network

```
services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_PASSWORD:root
      - POSTGRES_USER=root
      - POSTGRES_DB=ny_taxi
      - TZ=Asia/Jakarta
      - PGTZ=Asia/Jakarta
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    volumes:
      - ./data_pgadmin:/var/lib/pgadmin
    ports:
      - "8080:80"
```
