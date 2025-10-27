from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import zipfile
import glob
from google.cloud import (
    storage,
)  # klient GCS – musi być zainstalowany w obrazie Airflow

# --- KONFIGURACJA ŚCIEŻEK I GCS ---
# Lokalny folder tymczasowy w kontenerze Airflow (montowany jako ./temp w docker-compose)
LOCAL_DIR = "/opt/airflow/temp/netflix"

# Domyślne ścieżki plików (używane pomocniczo / jako fallback)
LOCAL_ZIP = os.path.join(LOCAL_DIR, "netflix.zip")
LOCAL_CSV = os.path.join(LOCAL_DIR, "netflix_titles.csv")

# Nazwa bucketu GCS i ścieżka docelowa pliku CSV
BUCKET_NAME = "airflow-and-spark-pipeline-raw"
DESTINATION_BLOB = "netflix/netflix_titles.csv"

# --- FUNKCJE WYKONYWANE W TASKACH ---

def download_from_kaggle(**context):
    """
    Pobiera najnowszy dataset Netflix z Kaggle do katalogu LOCAL_DIR.
    - Używa Kaggle API do uwierzytelnienia i pobrania pliku ZIP
    - Import pakietu 'kaggle' wykonywany lokalnie (lazy import), aby DAG mógł się parsować nawet bez zainstalowanego pakietu
    """
    os.makedirs(LOCAL_DIR, exist_ok=True)

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except Exception as e:
        raise RuntimeError(
            "Brak pakietu 'kaggle'. Zainstaluj go lub przebuduj obraz Airflow."
        ) from e

    api = KaggleApi()
    api.authenticate()

    # Pobranie zbioru jako plik ZIP (nie rozpakowujemy tutaj)
    api.dataset_download_files(
        "shivamb/netflix-shows", path=LOCAL_DIR, unzip=False
    )

    # Znalezienie najnowszego pliku ZIP w katalogu tymczasowym
    zip_files = glob.glob(os.path.join(LOCAL_DIR, "*.zip"))
    if not zip_files:
        raise FileNotFoundError(f"Brak pliku ZIP w {LOCAL_DIR} po pobraniu.")
    zip_path = max(zip_files, key=os.path.getctime)

    print(f"Pobrano plik ZIP: {zip_path}")


def unzip_file(**context):
    """
    Rozpakowuje najnowszy plik ZIP w LOCAL_DIR i wyszukuje plik CSV.
    - Po rozpakowaniu zapisuje ścieżkę CSV do XCom, aby kolejne zadania mogły jej użyć
    """
    zip_files = glob.glob(os.path.join(LOCAL_DIR, "*.zip"))
    if not zip_files:
        raise FileNotFoundError("Nie znaleziono pliku ZIP do rozpakowania.")
    zip_path = max(zip_files, key=os.path.getctime)

    # Rozpakowanie ZIP
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(LOCAL_DIR)

    # Wyszukiwanie pliku CSV w rozpakowanym katalogu
    csv_candidates = glob.glob(os.path.join(LOCAL_DIR, "*.csv"))
    if not csv_candidates:
        raise FileNotFoundError("Po rozpakowaniu nie znaleziono pliku CSV.")
    csv_path = max(csv_candidates, key=os.path.getctime)

    # Przekazanie ścieżki CSV do XCom
    context["ti"].xcom_push(key="netflix_csv_path", value=csv_path)
    print(f"Rozpakowano {zip_path} -> znaleziono CSV: {csv_path}")


def upload_to_gcs(**context):
    """
    Wgrywa plik CSV do Google Cloud Storage.
    - Ścieżka do pliku pobierana jest z XCom (lub fallback do LOCAL_CSV)
    - Wymaga skonfigurowanego uwierzytelnienia GCP w kontenerze Airflow
    """
    csv_path = context["ti"].xcom_pull(key="netflix_csv_path")
    if not csv_path or not os.path.exists(csv_path):
        csv_path = LOCAL_CSV
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Plik CSV nie istnieje: {csv_path}")

    # Inicjalizacja klienta GCS i upload pliku
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(DESTINATION_BLOB)
    blob.upload_from_filename(csv_path)

    print(
        f"Plik {csv_path} przesłany do gs://{BUCKET_NAME}/{DESTINATION_BLOB}"
    )


# --- DEFINICJA DAG-a ---
# DAG uruchamia 3-etapowy proces:
# 1. Pobranie danych z Kaggle
# 2. Rozpakowanie pliku ZIP
# 3. Upload CSV do GCS
with DAG(
    dag_id="download_netflix_dag",
    start_date=datetime(2025, 10, 1),  # data startu harmonogramu
    schedule=None,  # brak automatycznego harmonogramu, uruchamiany ręcznie lub przez inny DAG
    catchup=False,  # pomijanie historycznych runów
    tags=["kaggle", "netflix", "example"],
) as dag:

    # Zadanie 1: pobranie datasetu z Kaggle
    t1 = PythonOperator(
        task_id="download_from_kaggle",
        python_callable=download_from_kaggle,
    )

    # Zadanie 2: rozpakowanie ZIP i lokalizacja CSV
    t2 = PythonOperator(
        task_id="unzip_file",
        python_callable=unzip_file,
    )

    # Zadanie 3: przesłanie CSV do GCS
    t3 = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    # Definicja kolejności zadań w DAG-u
    t1 >> t2 >> t3
