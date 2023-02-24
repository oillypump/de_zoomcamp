from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("de-zoomcamp")
    # gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"")
    # return Path(f".../data/{gcs_path}")
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(
        f"pre: missing pasengger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0)
    print(
        f"post: missing pasengger count: {df['passenger_count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """write  dataframe to bigQuerry"""

    gcp_credentials_block = GcpCredentials.load("de-zoomcamp-creds")

    df.to_gbq(
        destination_table="dezoomcampdataset.rides",
        project_id="de-analytics-378504",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
