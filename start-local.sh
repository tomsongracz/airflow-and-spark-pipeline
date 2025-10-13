#!/bin/bash
# ==========================
# Skrypt start-local.sh
# Uruchamia wszystkie kontenery (Airflow + Spark)
# Obsługuje opcjonalny build obrazów (flaga --build)
# ==========================

# Flaga do budowania obrazów
BUILD_IMAGES=false

# Sprawdź, czy podano flagę --build
if [[ "$1" == "--build" ]]; then
  BUILD_IMAGES=true
fi

# 1. Zatrzymanie i usunięcie starych kontenerów
echo ">>> Zatrzymywanie istniejących kontenerów..."
docker compose -f airflow/docker-compose.yaml down
docker compose -f spark/docker-compose.yaml down

# 2. Opcjonalne budowanie obrazów
if [ "$BUILD_IMAGES" = true ]; then
  echo ">>> Budowanie obrazów..."
  docker compose -f airflow/docker-compose.yaml build
  docker compose -f spark/docker-compose.yaml build
else
  echo ">>> Pomijam budowanie obrazów (używam istniejących)..."
fi

# 3. Uruchomienie kontenerów Airflow
echo ">>> Uruchamianie kontenerów Airflow..."
docker compose -f airflow/docker-compose.yaml up -d

# 4. Uruchomienie kontenerów Spark
echo ">>> Uruchamianie kontenerów Spark..."
docker compose -f spark/docker-compose.yaml up -d

# 5. Sprawdzenie statusu kontenerów
echo ">>> Lista działających kontenerów:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ">>> Wszystkie kontenery Airflow i Spark powinny być uruchomione."
echo ">>> Airflow UI:  http://localhost:8080"
echo ">>> Spark Master UI: http://localhost:8081"
