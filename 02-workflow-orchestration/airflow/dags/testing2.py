from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="testing2",
    schedule=None,  # Hanya jalankan manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test"],
)
def simple_logging_dag():
    @task()
    def task_pertama():
        name = "Ryano"
        print("-----------------------------------------")
        print(f"PRINT DARI TASK 1: Hello, {name}!")
        print("Menggunakan print() kurang disarankan.")
        print("-----------------------------------------")
        return name

    @task()
    def task_kedua(input_data):
        print("-----------------------------------------")
        print(f"PRINT DARI TASK 2: Menerima input -> {input_data}")
        print("Menggunakan print() kurang disarankan.")
        print("-----------------------------------------")

    nama = task_pertama()
    task_kedua(nama)


simple_logging_dag()
