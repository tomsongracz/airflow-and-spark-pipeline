from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower

if __name__ == "__main__":
    spark = SparkSession.builder.appName("NetflixETL").getOrCreate()

    # === 1. Wczytaj dane z RAW (CSV z GCS) ===
    raw_path = "gs://airflow-and-spark-pipeline-raw/netflix/netflix_titles.csv"
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(raw_path)
    )
    print(f"Wczytano {df.count()} wierszy z GCS")  # Debug: liczba wierszy

    # === 2. Proste czyszczenie / transformacja ===
    # - Usuń wiersze z jakimikolwiek NULL w kolumnach
    # - Rzutowanie release_year na int
    # - Przytnij spacje wokół tekstów i ujednolić case dla tytułu (pomaga w deduplikacji)
    # - Usuń kolumnę "rating"
    df_clean = (
        df.dropna()  # usuwa wiersze z jakimikolwiek NULL
        .withColumn("release_year", col("release_year").cast("int"))
        .withColumn("title", trim(col("title")))
        .withColumn("title_lower", lower(col("title")))
        .drop("rating")  # Usuń kolumnę "rating"
    )
    print(f"Po czyszczeniu: {df_clean.count()} wierszy")

    # === 3. Usuwanie duplikatów ===
    # Strategia:
    # - Jeśli istnieje kolumna 'show_id', traktujemy ją jako unikalny identyfikator i usuwamy duplikaty po niej.
    # - W przeciwnym razie używamy kombinacji kolumn (title_lower, release_year, type) do wykrycia duplikatów.
    # - Zachowujemy pierwszy wystąpienie każdej grupy duplikatów (domyślne zachowanie dropDuplicates).
    cols = df_clean.columns
    if "show_id" in cols:
        dedup_subset = ["show_id"]
        print("Usuwanie duplikatów na podstawie ['show_id']")
    else:
        # fallback - kombinacja pól powinna dobrze rozróżniać tytuły
        dedup_subset = ["title_lower", "release_year", "type"]
        print(
            "Usuwanie duplikatów na podstawie ['title_lower', 'release_year', 'type'] (awaryjnie)"
        )

    df_dedup = df_clean.dropDuplicates(subset=dedup_subset)
    print(
        f"Po usunięciu duplikatów: {df_dedup.count()} wierszy (usunięto {df_clean.count() - df_dedup.count()} duplikatów)"
    )

    # === 4. Przygotowanie do zapisu: usuwamy pomocnicze kolumny ===
    # Usuwamy kolumnę pomocniczą title_lower przed zapisem, jeśli istnieje
    if "title_lower" in df_dedup.columns:
        df_to_write = df_dedup.drop("title_lower")
    else:
        df_to_write = df_dedup

    # === 5. Zapisz do processed zone (Parquet) ===
    processed_path = "gs://airflow-and-spark-pipeline-processed/netflix_clean"
    df_to_write.coalesce(1).write.mode("overwrite").parquet(processed_path)
    print(f"Dane po czyszczeniu zapisano do {processed_path}")

    spark.stop()
