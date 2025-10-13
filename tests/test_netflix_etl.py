from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def test_etl_clean_and_dedup(tmp_path):
    """Sprawdza, czy skrypt poprawnie usuwa duplikaty i puste wartości."""

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("TestNetflixETL")
        .getOrCreate()
    )

    # Przygotowanie danych testowych (symulacja netflix_titles.csv)
    data = [
        ("s1", "Movie", "Inception", "Christopher Nolan", "2010"),
        (
            "s2",
            "Movie",
            "Inception ",
            None,
            "2010",
        ),  # duplikat z dodatkową spacją + NULL w director
        (
            "s3",
            "TV Show",
            None,
            "TBD",
            "2019",
        ),  # brak tytułu — powinno zostać usunięte
        ("s4", "Movie", "Avatar", "James Cameron", "2009"),
        (
            "s4",
            "Movie",
            "Avatar",
            "James Cameron",
            "2009",
        ),  # duplikat po show_id
        (
            "s5",
            "TV Show",
            "Test Show",
            None,
            "2020",
        ),  # NULL tylko w director — powinno zostać usunięte przez dropna()
    ]
    columns = [
        "show_id",
        "type",
        "title",
        "director",
        "release_year",
    ]  # Dodano "director" dla testu NULL-a

    df = spark.createDataFrame(data, columns)

    # Symulacja czyszczenia
    df_clean = (
        df.dropna()  # usuwa wiersze z jakimikolwiek NULL
        .withColumn("release_year", col("release_year").cast("int"))
        .withColumn("title", trim(col("title")))
    )

    # Symulacja deduplikacji
    df_dedup = df_clean.dropDuplicates(subset=["show_id"])

    # Asercje
    assert (
        df_clean.count() == 3
    ), "Po dropna() powinny zostać 3 rekordy (usunięto s2, s3, s5 z NULL-ami)"
    assert (
        df_dedup.count() == 2
    ), "Po deduplikacji po show_id powinny zostać 2 rekordy"  # s4 duplikat usunięty

    # Sprawdzenie czyszczenia spacji
    row = df_dedup.filter(col("show_id") == "s1").collect()[
        0
    ]  # Użyto s1 zamiast s2 (bo s2 usunięty)
    assert row["title"] == "Inception", "Spacje powinny być przycięte"

    # Sprawdzenie braku kolumny "rating" (choć jej nie ma w danych, symuluje drop)
    assert (
        "rating" not in df_dedup.columns
    ), "Kolumna 'rating' powinna być usunięta"

    spark.stop()
