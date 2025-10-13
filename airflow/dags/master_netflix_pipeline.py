from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import pendulum

# -------------------------------------------------------------------
# MASTER DAG: Orkiestracja całego pipeline'u Netflix
# -------------------------------------------------------------------
# Harmonogram: codziennie o 12:00 czasu polskiego (Europe/Warsaw)
# Kolejność:
#   1. download_netflix_dag      -> pobranie danych z Kaggle do GCS
#   2. netflix_etl_dag           -> czyszczenie i przetwarzanie Spark
#   3. load_netflix_to_bq_dag    -> ładowanie danych do BigQuery
# -------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

local_tz = pendulum.timezone("Europe/Warsaw")

with DAG(
    dag_id="master_netflix_pipeline",
    description="Master DAG orkiestrujący wszystkie etapy pipeline’u Netflix (download → ETL → BigQuery)",
    start_date=datetime(2025, 10, 10, tzinfo=local_tz),
    schedule="00 12 * * *",  # codziennie o 12:00 czasu polskiego
    catchup=False,
    default_args=default_args,
    tags=["netflix", "pipeline", "orchestration"],
) as dag:

    # ----------------------------------------------------------------
    # 1. Trigger: download_netflix_dag
    # ----------------------------------------------------------------
    trigger_download = TriggerDagRunOperator(
        task_id="trigger_download_netflix_dag",
        trigger_dag_id="download_netflix_dag",
        wait_for_completion=True,  # czekaj aż DAG się zakończy
        poke_interval=60,  # sprawdzaj co 60 sekund
    )

    # ----------------------------------------------------------------
    # 2. Trigger: netflix_etl_dag
    # ----------------------------------------------------------------
    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_netflix_etl_dag",
        trigger_dag_id="netflix_etl_dag",
        wait_for_completion=True,
        poke_interval=60,
    )

    # ----------------------------------------------------------------
    # 3. Trigger: load_netflix_to_bq_dag
    # ----------------------------------------------------------------
    trigger_load_bq = TriggerDagRunOperator(
        task_id="trigger_load_netflix_to_bq_dag",
        trigger_dag_id="load_netflix_to_bq_dag",
        wait_for_completion=True,
        poke_interval=60,
    )

    # ----------------------------------------------------------------
    # Kolejność zależności: download → etl → load
    # ----------------------------------------------------------------
    trigger_download >> trigger_etl >> trigger_load_bq
