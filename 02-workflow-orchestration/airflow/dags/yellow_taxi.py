import pandas as pd
import hashlib
import re
import requests
import io
from datetime import datetime
from airflow.exceptions import AirflowSkipException
from airflow.sdk import dag, task


@dag(
    dag_id="yellow_taxi",
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=True,
    tags=["taxi", "yellow"],
)
def yellow_taxi_pipeline():
    @task()
    def extract_and_transform_data(color: str, **context) -> str:
        logical_date = context["logical_date"]
        year = logical_date.year
        month = logical_date.month

        file_name = f"{color}_tripdata_{year}-{month:02d}.parquet"
        source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

        print(f"On Processing Data : {year}-{month:02d}")

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
            print(f"Error loading data: {source_url}: {e}")
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
        local_path = f"{output_dir}/{file_name}"
        df.to_parquet(local_path, index=False)

        return local_path

    color_val = "yellow"

    extract_and_transform_data(color_val)


yellow_taxi_pipeline()
