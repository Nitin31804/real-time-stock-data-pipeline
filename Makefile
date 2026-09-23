.PHONY: up down status logs test lint validate

up:
	docker compose up --build -d

down:
	docker compose down

status:
	docker compose ps

logs:
	docker compose logs -f --tail=200

test:
	python -m pytest -m "not integration"

lint:
	python -m ruff check producer spark_processor dashboard tests benchmarks

validate:
	docker compose config --quiet
	python -m compileall -q producer spark_processor dashboard tests benchmarks
