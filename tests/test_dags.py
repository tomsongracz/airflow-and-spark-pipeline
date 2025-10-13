import os
import importlib.util
from airflow.models import DAG

# Ścieżka do katalogu z plikami DAG-ów (relatywna do lokalizacji tego pliku testowego)
DAGS_DIR = os.path.join(os.path.dirname(__file__), "../airflow/dags")


def _load_module_from_file(filepath):
    """
    Pomocnicza funkcja do ładowania modułu Python z pliku.
    Używa importlib do dynamicznego importu bez dodawania do sys.path.
    """
    spec = importlib.util.spec_from_file_location("temp_module", filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_all_dags_importable():
    """Sprawdza, czy wszystkie pliki DAG-ów dają się zaimportować bez błędu."""
    # Iteracja po wszystkich plikach .py w katalogu DAG-ów
    for filename in os.listdir(DAGS_DIR):
        if filename.endswith(".py"):
            path = os.path.join(DAGS_DIR, filename)
            try:
                _load_module_from_file(path)
            except Exception as e:
                raise AssertionError(
                    f"Nie udało się zaimportować DAG-a {filename}: {e}"
                )


def test_dag_objects_exist():
    """Weryfikuje, że każdy plik DAG zawiera co najmniej jeden obiekt typu DAG."""
    # Iteracja po plikach .py i ładowanie modułów
    for filename in os.listdir(DAGS_DIR):
        if not filename.endswith(".py"):
            continue
        module = _load_module_from_file(os.path.join(DAGS_DIR, filename))
        # Wyciągnięcie wszystkich obiektów DAG z modułu
        dags = [v for v in vars(module).values() if isinstance(v, DAG)]
        assert dags, f"Plik {filename} nie zawiera obiektu DAG"
        # Walidacja każdego DAG-a: dag_id i liczba tasków
        for dag in dags:
            assert dag.dag_id, f"DAG w {filename} nie ma ustawionego dag_id"
            assert len(dag.tasks) > 0, f"DAG {dag.dag_id} nie ma żadnych zadań"


def test_master_pipeline_dependencies():
    """Sprawdza, czy master DAG ma zależność download -> etl -> load."""
    # Ścieżka do pliku master DAG-a
    master_path = os.path.join(DAGS_DIR, "master_netflix_pipeline.py")
    if not os.path.exists(master_path):
        return

    # Ładowanie modułu i wyciąganie DAG-a
    module = _load_module_from_file(master_path)
    dags = [v for v in vars(module).values() if isinstance(v, DAG)]
    assert dags, "Brak DAG-a master_netflix_pipeline"
    master_dag = dags[0]

    # Lista ID tasków w master DAG-u
    task_ids = [t.task_id for t in master_dag.tasks]
    assert "trigger_download_netflix_dag" in task_ids
    assert "trigger_netflix_etl_dag" in task_ids
    assert "trigger_load_netflix_to_bq_dag" in task_ids

    # Sprawdzenie zależności upstream (kolejność triggerów)
    etl_task = master_dag.get_task("trigger_netflix_etl_dag")
    load_task = master_dag.get_task("trigger_load_netflix_to_bq_dag")
    assert "trigger_download_netflix_dag" in [
        t.task_id for t in etl_task.upstream_list
    ]
    assert "trigger_netflix_etl_dag" in [
        t.task_id for t in load_task.upstream_list
    ]
