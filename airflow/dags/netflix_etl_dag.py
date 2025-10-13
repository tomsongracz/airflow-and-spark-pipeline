from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from datetime import datetime

# Domyślne argumenty dla DAG-a
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),  # data rozpoczęcia harmonogramu
    "retries": 1,  # liczba ponownych prób w razie błędu
}

# Definicja głównego DAG-a uruchamiającego zadanie ETL w Spark
with DAG(
    dag_id="netflix_etl_dag",
    default_args=default_args,
    schedule=None,  # brak automatycznego harmonogramu, uruchamiany ręcznie lub przez inny DAG
    catchup=False,  # nie uruchamiaj historycznych runów
    tags=["spark", "netflix"],  # tagi pomocne przy filtrowaniu w UI
) as dag:

    # Zadanie uruchamiające aplikację Spark do przetwarzania danych Netflix
    netflix_etl = SparkSubmitOperator(
        task_id="run_netflix_etl",  # unikalna nazwa zadania w DAG-u
        application="/opt/spark-apps/netflix_etl.py",  # ścieżka do skryptu PySpark wykonywanego w kontenerze Spark
        conn_id="spark_local",  # ID połączenia do klastra Spark zdefiniowanego w Airflow
        executor_cores=2,  # liczba rdzeni dla każdego executora
        executor_memory="2g",  # ilość pamięci RAM dla executora
        driver_memory="1g",  # ilość pamięci RAM dla drivera
        verbose=True,  # logowanie szczegółowe
        jars="/opt/airflow/jars/gcs-connector.jar",  # dodatkowy JAR umożliwiający obsługę GCS w Spark
        # Konfiguracja dostępu do Google Cloud Storage z poziomu Spark
        conf={
            "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            "spark.hadoop.google.cloud.auth.service.account.enable": "true",  # włączenie uwierzytelniania przez zmienne środowiskowe
        },
        # Zmienne środowiskowe przekazywane do kontenera Spark
        # (opcjonalne – jeśli już ustawione w docker-compose, nie trzeba ich duplikować)
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/gcp/gcp-key.json"
        },
    )
