# Airflow and Spark ETL Pipeline on GCS Lakehouse

End-to-end pipeline danych z orkiestracją w **Apache Airflow**, przetwarzaniem w **Apache Spark** i architekturą **Data Lakehouse** w **Google Cloud Platform (GCS + BigQuery)**. 
Projekt demonstruje pełny cykl ETL na danych Netflix z Kaggle — od pobrania, przez czyszczenie i deduplikację w **Spark**, po załadowanie danych do **BigQuery**.

---

## 🛠 Stack technologiczny

| **Komponent**       | **Technologia** | **Rola** |
|----------------------|-----------------|-----------|
| **Orkiestracja**    | Apache Airflow 2.8.1 (Docker, CeleryExecutor) | DAG-i, scheduling, triggerowanie, monitoring (UI: `localhost:8080`) |
| **Przetwarzanie**   | Apache Spark 3.5.3 (PySpark, Python 3.10.12, Docker cluster: master + worker) | ETL: czyszczenie, deduplikacja, zapis Parquet (UI: `localhost:8081`) |
| **Data Lake**       | Google Cloud Storage (GCS) | Warstwy Raw / Processed / Temp (buckety: `-raw`, `-processed`, `-temp`) |
| **Hurtownia danych**| Google BigQuery | Ładowanie Parquet, schemat tabeli, analityka |
| **CI/CD**           | GitHub Actions | Testy Python, linting (Black/Flake8), build Docker |
| **Testy**           | pytest + Makefile (Python 3.10.12) | Testy DAG-ów (import, zależności) i Spark ETL (czyszczenie / duplikaty) |
| **Integracje**      | Kaggle API, GCS Connector JAR, Java 17 (dla SparkSubmitOperator) | Pobieranie danych, uwierzytelnianie GCP |

> Kod sformatowany za pomocą **Black** + **Flake8** (ignorowanie `E501` dla długich linii).

---

## 🚀 Opis projektu

Codzienny pipeline orkiestrowany przez **Apache Airflow** pobiera surowe dane (CSV z **Kaggle**, ok. 1GB+), przetwarza je w **Apache Spark**, a następnie ładuje do **BigQuery**.  
Proces składa się z czterech głównych **DAG-ów**:

- **`download_netflix_dag`** — pobiera i rozpakowuje dataset z Kaggle, uploaduje plik CSV do **Raw Zone** w **GCS**.  
- **`netflix_etl_dag`** — uruchamia job **Spark** odpowiedzialny za czyszczenie danych (usuwanie `NULL`, duplikatów, normalizacja) i zapisuje dane w formacie **Parquet** do **Processed Zone** w **GCS**.  
- **`load_to_bq_dag`** — ładuje przetworzone dane z **GCS** do tabeli w **BigQuery**.  
- **`master_netflix_pipeline`** — nadrzędny DAG, który sekwencyjnie uruchamia powyższe sub-DAG-i (**download → ETL → load**) zgodnie z harmonogramem codziennym o **12:00 (czas polski)**.

Projekt został zrealizowany **lokalnie w Dockerze** (Airflow + Spark), symulując architekturę **Data Lakehouse** w **Google Cloud Platform (GCP)**.  
W środowisku produkcyjnym **Airflow** mógłby działać w **Cloud Composer**, a **Spark** w **Dataproc Serverless** — pipeline wykorzystuje te same koncepcje orkiestracji, przetwarzania oraz warstw danych (**Raw → Processed → Warehouse**).

---

## 🏗️ Architektura danych

Pipeline opiera się na koncepcji **Medallion Architecture (Lakehouse)**, obejmującej warstwy danych od surowych po analityczne:

- **Raw Zone** — (`GCS bucket: airflow-and-spark-pipeline-raw`)  
  Surowe pliki **CSV** pobrane z **Kaggle**.
- **Temp Zone** — (`GCS bucket: airflow-and-spark-pipeline-temp`)  
  Dane pośrednie / staging (obecnie niewykorzystywana, lecz gotowa na przyszłe rozszerzenia — np. joiny między źródłami lub inkrementalne loady).
- **Processed Zone** — (`GCS bucket: airflow-and-spark-pipeline-processed`)  
  Oczyszczone dane w formacie **Parquet**, będące wynikiem joba **Spark**.
- **Warehouse** — (`BigQuery dataset: airflow-and-spark-pipeline.netflix_data.titles_clean`)  
  Dane gotowe do analizy, z jasno zdefiniowanym schematem (`STRING` / `INTEGER` dla kolumn takich jak `show_id`, `title`, `release_year`).

---

### 📊 Schemat przepływu danych

```text
Kaggle API → [download_netflix_dag] → GCS Raw (CSV)
                          ↓
GCS Raw → [netflix_etl_dag + Spark] → GCS Processed (Parquet)
                          ↓
GCS Processed → [load_to_bq_dag] → BigQuery Warehouse
```

---

## 📁 Struktura projektu

```text
airflow-and-spark-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yaml              # GitHub Actions CI/CD
├── airflow/
│   ├── dags/
│   │   ├── download_netflix_dag.py  # Pobieranie z Kaggle → GCS
│   │   ├── netflix_etl_dag.py       # Trigger Spark ETL
│   │   ├── load_to_bq_dag.py        # Load do BigQuery
│   │   └── master_netflix_pipeline.py # Master orchestrator
│   ├── logs/                       # Logi Airflow
│   ├── temp/                       # Tymczasowe dane (montowane w Docker)
│   │   └── netflix/
│   │       ├── netflix_titles.csv
│   │       └── netflix-shows.zip
│   ├── gcp-key.json               # Klucz GCP (zignorowany w .gitignore)
│   ├── kaggle-key.json            # Klucz Kaggle (zignorowany w .gitignore)
│   ├── gcs-connector.jar          # JAR dla GCS w Spark
│   ├── requirements.txt           # Deps Python: kaggle, pyspark, providers
│   ├── docker-compose.yaml        # Kontenery: postgres, redis, scheduler, webserver, worker, init
│   └── Dockerfile                 # Custom obraz Airflow + Java
├── spark/
│   ├── apps/
│   │   └── netflix_etl.py         # PySpark job: clean + dedup + Parquet
│   ├── data/                      # Dane testowe (opcjonalne)
│   ├── docker-compose.yaml        # Spark master + worker (wspólna sieć z Airflow)
│   └── Dockerfile                 # Custom Spark + GCS connector + delta-spark
├── tests/
│   ├── test_dags.py               # Testy importu i zależności DAG-ów
│   └── test_netflix_etl.py        # Testy Spark ETL (pytest, z mock danymi)
├── start-local.sh                 # Skrypt: down + (opcjonalny) build + up Airflow + Spark
├── .gitignore                     # Ignoruje logi, klucze, temp, dane
├── Makefile                       # make test (uruchamia pytest)
└── README.md                      

```

---

## ✨ Jak to działa

**Master DAG** uruchamia się codziennie o **12:00 (Europe/Warsaw)** i triggeruje sub-DAG-i sekwencyjnie (`wait_for_completion=True`).

### 1. Download
- **PythonOperator** pobiera plik ZIP z **Kaggle**, rozpakowuje CSV i uploaduje do **GCS Raw**.  
- Ścieżki plików przekazywane są między taskami za pomocą **XCom**.

### 2. ETL
- **SparkSubmitOperator** uruchamia `netflix_etl.py`.  
- Proces ETL:
  - Czyta CSV z GCS
  - Czyszczenie danych: `dropna()`, `trim()`, `lower()`, usuwa kolumnę `rating`
  - Usuwa duplikaty po `show_id`
  - Zapisuje wynik w formacie **Parquet** do **Processed Zone** w GCS  
- Konfiguracja połączenia z GCS odbywa się przez **conf / zmienne środowiskowe**.

### 3. Load
- **GCSToBigQueryOperator** ładuje Parquet do **BigQuery**:
  - Tryb: **overwrite**
  - Schemat tabeli zdefiniowany w DAG-u

### Monitoring
- **Airflow UI**: DAG runs, logi, status tasków  
- **Spark UI**: statystyki executorów  
- **Retries** i `email_on_failure` skonfigurowane w `default_args` DAG-a

---

## 🧩 Wymagania

### Środowisko:
**WSL2 + Docker Desktop** (lub Linux/Mac)

### Java 17:  
**Dla `SparkSubmitOperator` i testów:**

```bash
sudo apt install openjdk-17-jre-headless -y
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### Python dependencies (dla testów)
```bash
pip install apache-airflow==2.8.1 pytest
pip install -r airflow/requirements.txt  # kaggle, pyspark, providers
```

---

## ☁️ GCP Setup

### 1. Włącz wymagane API
```bash
gcloud services enable storage.googleapis.com \
bigquery.googleapis.com \
iam.googleapis.com
```
### 2. Utwórz buckety w Cloud Storage
```bash
gsutil mb -l europe-central2 gs://airflow-and-spark-pipeline-raw/
gsutil mb -l europe-central2 gs://airflow-and-spark-pipeline-processed/
gsutil mb -l europe-central2 gs://airflow-and-spark-pipeline-temp/
```
### 3. Utwórz Service Account i przypisz role
```bash
gcloud iam service-accounts create airflow-spark-sa \
--display-name "Airflow and Spark Service Account"

gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
--member="serviceAccount:airflow-spark-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
--role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding <YOUR_PROJECT_ID> \
--member="serviceAccount:airflow-spark-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com" \
--role="roles/bigquery.jobUser"
```
💡 Alternatywnie: możesz użyć uproszczonej roli:
```bash
--role="roles/infrastructure.admin"
```
### 4. Pobierz klucz i zapisz w katalogu airflow/
```bash
gcloud iam service-accounts keys create airflow/gcp-key.json \
--iam-account=airflow-spark-sa@<YOUR_PROJECT_ID>.iam.gserviceaccount.com
```
### 5. Utwórz dataset w BigQuery
```bash
bq --location=europe-central2 mk \
--dataset <YOUR_PROJECT_ID>:airflow-and-spark-pipeline
```
✅ Po wykonaniu powyższych kroków:

- Masz skonfigurowane API, bucket’y i dataset.

- Plik gcp-key.json znajduje się w katalogu airflow/ i będzie używany przez Airflow i Spark.

---

## 🧠 Kaggle Setup

### 1. Pobierz klucz API z Kaggle
1. Wejdź na [https://www.kaggle.com/settings](https://www.kaggle.com/settings)  
2. Przewiń do sekcji **API**  
3. Kliknij **"Create New API Token"**  
   → Plik `kaggle.json` zostanie pobrany automatycznie.

### 2. Zapisz klucz w katalogu `airflow/`
Zmień nazwę pobranego pliku i przenieś go:
```bash
mv ~/Downloads/kaggle.json airflow/kaggle-key.json
```

---

## 🐳 Uruchomienie
### 1. Stwórz sieć Docker
```bash
docker network create airflow-spark-network
```

### 2. Uruchom kontenery
```bash
chmod +x start-local.sh
./start-local.sh --build  # Pierwszy raz: buduje obrazy
# Lub bez build, jeśli obrazy już istnieją:
./start-local.sh
```
**UI**
- Airflow UI: http://localhost:8080
- Spark UI: http://localhost:8081

---

## ⚙️ Konfiguracja połączeń w Airflow UI

W panelu **Admin → Connections** należy dodać poniższe połączenia:

| Conn ID               | Conn Type      | Host / Path                   | Additional Info                                                                 |
|------------------------|----------------|--------------------------------|----------------------------------------------------------------------------------|
| `spark_local`          | Spark          | `spark://spark-master:7077`    | —                                                                                |
| `google_cloud_default` | Google Cloud   | Keyfile Path: `/opt/airflow/gcp/gcp-key.json`<br>Project ID: `airflow-and-spark-pipeline` | 

---

## 🚦 Uruchom pipeline

- W UI wyzwól ręcznie DAG: master_netflix_pipeline
(lub poczekaj na schedule)

- Sprawdź dane w bucketach GCS i tabelach BigQuery.

---

## 🧪 Testy

Uruchom wszystkie testy:
```bash
make test
```

Lub ręcznie:
```bash
pytest tests/test_netflix_etl.py -v
pytest tests/test_dags.py -v
```

**Pliki testów**

- test_dags.py – sprawdza import DAG-ów i zależności (np. master → sub-DAG-i).

- test_netflix_etl.py – symuluje działanie Sparka (dropna, dedup, trim) z mock danymi.

---

## ✅ Gotowe!

**Po poprawnej konfiguracji:**

- Airflow automatycznie orchestruje taski Spark i GCP.

- Pipeline zapisuje dane z Kaggle do GCS i BigQuery.

- Możesz rozwijać DAG-i lokalnie lub w środowisku produkcyjnym GCP.

---

## 🔄 CI/CD

**GitHub Actions (ci.yaml): Automatycznie na push/PR do main:**

- Testy Python (pytest via Makefile).
- Linting: Black (format check), Flake8 (styl, ignore E501).
- Build Docker (Airflow + Spark) – weryfikacja bez run.

## 🌟 Rozszerzenia i przemyślenia

- **Delta Lake**: Obecny Dockerfile Spark instaluje `delta-spark==3.2.0` – łatwo ulepszyć ETL o ACID transactions i time travel (np. `df.write.format("delta").save()` zamiast Parquet), dodając wersjonowanie do Processed Zone.  
- **Temp Zone**: Struktura bucketa gotowa na staging (np. dane pośrednie przed joinami lub inkrementalnymi loadami do BigQuery).  
- **Produkcja**: Lokalny setup symuluje chmurę; migracja: Airflow do Cloud Composer, Spark do Dataproc (z autoscaling). Wspólna sieć Docker pokazuje świadomość konteneryzacji (Kubernetes w GCP).

---

## 👤 Autor
Projekt przygotowany w celach edukacyjnych i demonstracyjnych.
Możesz mnie znaleźć na GitHubie: [tomsongracz](https://github.com/tomsongracz)
  










