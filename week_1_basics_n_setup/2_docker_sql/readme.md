<!-- video source  -->

# source video

https://www.youtube.com/watch?v=B1WwATwf-vY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=8&ab_channel=DataTalksClub%E2%AC%9B

docker run -it \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_USER="root" \
-e POSTGRES_DB="ny_taxi" \
-e TZ="Asia/Jakarta" \
-e PGTZ="Asia/Jakarta" \
-p 5432:5432 \
-v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
postgres:13

docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
dpage/pgadmin4

# docker network

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

docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=de-network \
--name de-pg-admin \
dpage/pgadmin4

# ingest

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
 --user=root \
 --password=root \
 --host=localhost \
 --port=5432 \
 --db=ny_taxi \
 --table_name=yellow_taxi_trips \
 --url=${URL}

# docker build

docker build -t taxi_ingest:v001 .

# execute

make sftp : $python3 -m http.server

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

URL="http://172.31.112.182:8000/yellow_tripdata_2021-01.csv"

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
