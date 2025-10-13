from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime, timedelta, timezone

# Domyślne argumenty dla DAG-a
default_args = {
    "owner": "airflow",
    "start_date": datetime(
        2025, 1, 1, tzinfo=timezone.utc
    ),  # użycie strefy czasowej jest wymagane
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Definicja głównego DAG-a do ładowania danych do BigQuery
with DAG(
    dag_id="load_netflix_to_bq_dag",
    default_args=default_args,
    schedule=None,  # brak automatycznego harmonogramu, uruchamiany ręcznie lub przez inny DAG
    catchup=False,
    tags=["bigquery", "netflix"],
) as dag:

    # Zadanie ładujące dane z Google Cloud Storage do BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="airflow-and-spark-pipeline-processed",  # bucket z przetworzonymi danymi
        source_objects=[
            "netflix_clean/*.parquet"
        ],  # lokalizacja plików Parquet w GCS
        destination_project_dataset_table="airflow-and-spark-pipeline:netflix_data.titles_clean",  # tabela docelowa w BigQuery
        schema_fields=[  # definicja schematu tabeli
            {"name": "show_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "title", "type": "STRING", "mode": "NULLABLE"},
            {"name": "director", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cast", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date_added", "type": "STRING", "mode": "NULLABLE"},
            {"name": "release_year", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "duration", "type": "STRING", "mode": "NULLABLE"},
            {"name": "listed_in", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        ],
        source_format="PARQUET",  # dane zapisane przez Spark są w formacie Parquet
        create_disposition="CREATE_IF_NEEDED",  # utwórz tabelę, jeśli nie istnieje
        write_disposition="WRITE_TRUNCATE",  # nadpisz dane przy każdym uruchomieniu
        gcp_conn_id="google_cloud_default",  # połączenie do GCP zdefiniowane w Airflow
        dag=dag,
    )

    # Tylko jedno zadanie w tym DAG-u – ładowanie do BigQuery
    load_to_bq
