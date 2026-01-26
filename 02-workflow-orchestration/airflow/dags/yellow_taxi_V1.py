import io
import os
from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import hashlib
import re
from sqlalchemy import create_engine, text
from airflow.exceptions import AirflowSkipException
import requests
import time


@dag(
    dag_id="yellow_taxi_V1",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["taxi", "yellow"],
)
def yellow_taxi_pipeline():
    @task()
    def extract_and_transform_data(color: str, year: int, month: int) -> str:
        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
        source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        print(f"On Extracting Data : {year}-{month:02d}")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        try:
            response = requests.get(source_url, headers=headers)
            if response.status_code != 200:
                print(
                    f"data {year}-{month:02d} not available (status code: {response.status_code}). skip this task."
                )
                raise AirflowSkipException(f"data {year}-{month:02d} not available")
            df = pd.read_parquet(io.BytesIO(response.content))
        except AirflowSkipException:
            raise
        except Exception as e:
            print(f"Error loading data from {source_url}, data {year}-{month:02d}: {e}")
            raise

        unique_combined = (
            df["VendorID"].astype(str)
            + df["tpep_pickup_datetime"].astype(str)
            + df["tpep_dropoff_datetime"].astype(str)
            + df["PULocationID"].astype(str)
            + df["DOLocationID"].astype(str)
            + df["fare_amount"].astype(str)
            + df["trip_distance"].astype(str)
        )
        df["unique_id"] = unique_combined.apply(
            lambda x: hashlib.md5(x.encode()).hexdigest()
        )
        df["file_name"] = file_name

        # 2. Reorder Columns
        col_uid = df.pop("unique_id")
        col_filename = df.pop("file_name")
        df.insert(0, "unique_id", col_uid)
        df.insert(1, "file_name", col_filename)

        # 3. Snake Case Formatting
        def to_snake_case(name):
            s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
            return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

        df.columns = [to_snake_case(col) for col in df.columns]

        output_dir = "/opt/airflow/config"
        local_file_path = f"{output_dir}/{file_name}"
        print(f"Saving transformed data to {local_file_path}")
        df.to_parquet(local_file_path, index=False)

        return local_file_path

    @task()
    def load_to_staging(local_file_path: str, color: str) -> str:
        engine = create_engine(
            "postgresql://dateng26:dateng26@postgres-dateng:5432/dateng"
        )
        table_name = f"staging_{color}_taxi_data"

        df = pd.read_parquet(local_file_path)
        total_rows = len(df)

        int_columns = [
            "vendor_id",
            "passenger_count",
            "ratecode_id",
            "pu_location_id",
            "do_location_id",
            "payment_type",
        ]

        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        query_create_table_staging = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                unique_id TEXT,
                file_name TEXT,
                vendor_id INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance numeric(10,2),
                ratecode_id INT,
                store_and_fwd_flag TEXT,
                pu_location_id INT,
                do_location_id INT,
                payment_type INT,
                fare_amount numeric(10,2),
                extra numeric(10,2),
                mta_tax numeric(10,2),
                tip_amount numeric(10,2),
                tolls_amount numeric(10,2),
                improvement_surcharge numeric(10,2),
                total_amount numeric(10,2),
                congestion_surcharge numeric(10,2),
                airport_fee numeric(10,2),
                cbd_congestion_fee numeric(10,2)
            )"""

        with engine.begin() as connection:
            connection.execute(text(query_create_table_staging))
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))

        print(f"Starting bulk load of {total_rows} rows into {table_name}...")
        start_time = time.time()

        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            buffer = io.StringIO()
            df.to_csv(buffer, index=False, header=False, sep="\t", na_rep="\\N")
            buffer.seek(0)

            cursor.copy_from(buffer, table_name, sep="\t", null="\\N")
            raw_conn.commit()
            end_time = time.time()
            duration = end_time - start_time
            rps = total_rows / duration if duration > 0 else total_rows

            print(f"Bulk load completed in {duration:.2f} seconds.")
            print(f"Rows per second: {rps:.2f}")
            print(f"Total rows loaded: {total_rows}")
        except Exception as e:
            raw_conn.rollback()
            print(f"Error during bulk load into {table_name}: {e}")
            raise
        finally:
            cursor.close()
            raw_conn.close()

        return table_name

    @task()
    def merge_target(local_file_path: str, table_name: str, color: str):
        engine = create_engine(
            "postgresql://dateng26:dateng26@postgres-dateng:5432/dateng"
        )

        source_table = table_name
        print(f"source_table: {source_table}")
        target_table = f"{color}_taxi_data"

        query_create_target_table = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                unique_id TEXT,
                file_name TEXT,
                vendor_id INT,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count INT,
                trip_distance numeric(10,2),
                ratecode_id INT,
                store_and_fwd_flag TEXT,
                pu_location_id INT,
                do_location_id INT,
                payment_type INT,
                fare_amount numeric(10,2),
                extra numeric(10,2),
                mta_tax numeric(10,2),
                tip_amount numeric(10,2),
                tolls_amount numeric(10,2),
                improvement_surcharge numeric(10,2),
                total_amount numeric(10,2),
                congestion_surcharge numeric(10,2),
                airport_fee numeric(10,2),
                cbd_congestion_fee numeric(10,2)
            )"""

        query_merge = f"""
            MERGE INTO {target_table} AS T
            USING {source_table} AS S
            ON T.unique_id = S.unique_id
            WHEN NOT MATCHED THEN
                INSERT (unique_id, file_name, vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee)
                VALUES (S.unique_id, S.file_name, S.vendor_id, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, S.passenger_count, S.trip_distance, S.ratecode_id, S.store_and_fwd_flag, S.pu_location_id, S.do_location_id, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.improvement_surcharge, S.total_amount, S.congestion_surcharge, S.airport_fee, S.cbd_congestion_fee)
            """

        query_create_index = f"""
            CREATE INDEX IF NOT EXISTS idx_{target_table}_unique_id
            ON {target_table} (unique_id);
        """

        with engine.begin() as connection:
            connection.execute(text(query_create_target_table))
            print(f"Target table {target_table} is ready.")
            connection.execute(text(query_create_index))
            print(f"Index on {target_table}.unique_id is ready.")
            connection.execute(text(query_merge))
            print(f"Merging data from {source_table} to {target_table} in progress...")
            connection.execute(text(f"TRUNCATE TABLE {table_name}"))
            print(f"Staging table {table_name} truncated.")

        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"Temporary file {local_file_path} deleted.")

    # Main Pipeline Flow
    color_val = "yellow"
    year_val = 2025
    month_val = 2

    file_path = extract_and_transform_data(
        color=color_val, year=year_val, month=month_val
    )
    staged = load_to_staging(local_file_path=file_path, color=color_val)
    merge_target(local_file_path=file_path, table_name=staged, color=color_val)


yellow_taxi_pipeline()
