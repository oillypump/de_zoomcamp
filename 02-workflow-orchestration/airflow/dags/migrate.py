# %%
import pandas as pd
import numpy as np
from tqdm import tqdm
import re
import hashlib
from sqlalchemy import text

# %%
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet

# %%
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
color = "yellow"
year = 2025
month = "02"
file_name = f"{color}_tripdata_{year}-{month}.parquet"
source_url = f"{url}{file_name}"

df = pd.read_parquet(source_url)
df

# %%
unique_combined = (
    df["VendorID"].astype(str)
    + df["tpep_pickup_datetime"].astype(str)
    + df["tpep_dropoff_datetime"].astype(str)
    + df["PULocationID"].astype(str)
    + df["DOLocationID"].astype(str)
    + df["fare_amount"].astype(str)
    + df["trip_distance"].astype(str)
)

# Hash semuanya sekaligus
df["unique_id"] = unique_combined.apply(lambda x: hashlib.md5(x.encode()).hexdigest())

# %%
df["file_name"] = file_name

# %%
df.sort_values(by=["tpep_dropoff_datetime"], ascending=True, inplace=True)
df

# %%
df.dtypes

# %%
df.shape

# %%
cast_data_types = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "file_name": "string",
    "unique_id": "string",
}

# %%
df = df.astype(cast_data_types)

# %%
df.dtypes

# %%
# 1. Cabut kolom dari posisi lama dan simpan ke variabel sementara
col_uid = df.pop("unique_id")
col_filename = df.pop("file_name")

# 2. Masukkan kembali ke posisi paling depan (index 0 dan 1)
df.insert(0, "unique_id", col_uid)
df.insert(1, "file_name", col_filename)

# %%
df.dtypes


# %%
def to_snake_case(name):
    # Tambahkan underscore sebelum huruf besar yang diawali huruf kecil (VendorID -> Vendor_ID)
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    # Pisahkan singkatan besar (Vendor_ID -> vendor_id)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


# %%
df.columns = [to_snake_case(col) for col in df.columns]

# %%
df.dtypes

# %%
from sqlalchemy import create_engine

# %%
engine = create_engine("postgresql://dateng26:dateng26@localhost:5432/dateng")

# %%
# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))

# %%
query_create_staging = f"""
CREATE TABLE IF NOT EXISTS staging_{color}_taxi_data (
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

# %%
with engine.begin() as conn:
    conn.execute(text(query_create_staging))
    print(f"Tabel staging_{color}_taxi_data created.")

# %%
chunk_size = 5000
table_name = f"staging_{color}_taxi_data"

total_chunks = int(np.ceil(len(df) / chunk_size))

with tqdm(total=total_chunks, desc="Load staging") as progressbar:
    for i, chunk in enumerate(range(0, len(df), chunk_size)):
        df_chunk = df.iloc[chunk : chunk + chunk_size]

        df_chunk.to_sql(
            name=table_name, con=engine, if_exists="append", index=False, method="multi"
        )

        progressbar.update(1)

print("Load data selesai!")

# %%


# %%
query_merge = f"""
MERGE INTO {color}_taxi_data AS S
USING staging_{color}_taxi_data AS T
ON S.unique_id = T.unique_id
WHEN NOT MATCHED THEN
    INSERT (unique_id, file_name, vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_location_id, do_location_id, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee, cbd_congestion_fee)
    VALUES (T.unique_id, T.file_name, T.vendor_id, T.tpep_pickup_datetime, T.tpep_dropoff_datetime, T.passenger_count, T.trip_distance, T.ratecode_id, T.store_and_fwd_flag, T.pu_location_id, T.do_location_id, T.payment_type, T.fare_amount, T.extra, T.mta_tax, T.tip_amount, T.tolls_amount, T.improvement_surcharge, T.total_amount, T.congestion_surcharge, T.airport_fee, T.cbd_congestion_fee)
    """
with engine.begin() as conn:
    result = conn.execute(text(query_merge))
    print(f"Berhasil memindahkan {result.rowcount} baris ke tabel target.")

# %%
query_truncate = f"TRUNCATE TABLE staging_{color}_taxi_data;"
with engine.begin() as conn:
    conn.execute(text(query_truncate))
    print(f"Tabel staging_{color}_taxi_data telah dikosongkan.")

# %%
