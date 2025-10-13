# Makefile do uruchamiania testów lokalnie
# Użyj: make test  # Odpali oba testy sekwencyjnie

test:
	pytest tests/test_netflix_etl.py -v && pytest tests/test_dags.py -v

.PHONY: test