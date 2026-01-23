from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="testing_v3",
    tags=["test"],
    description="DAG Hello World menggunakan Decorators",
    catchup=False,
)
def flow():
    @task
    def print_hello():
        print("Hello, World!")
        return "berhasil"

    print_hello()


# Memanggil fungsi DAG agar terdaftar di Airflow
flow()
