.PHONY: up down logs reset clean status kafka-topics db-check

## Start the entire pipeline
up:
	@echo "🚀 Starting Real-Time Stock Data Pipeline..."
	@cp -n .env.example .env 2>/dev/null || true
	docker compose up -d --build
	@echo ""
	@echo "✅ Pipeline is running!"
	@echo "   Dashboard    → http://localhost:5000"
	@echo "   Spark UI     → http://localhost:8080"
	@echo "   Kafka        → localhost:29092"
	@echo "   PostgreSQL   → localhost:5432"

## Stop all containers
down:
	docker compose down

## Tail logs from all services
logs:
	docker compose logs -f --tail=50

## Follow logs for a specific service: make log SERVICE=producer
log:
	docker compose logs -f --tail=100 $(SERVICE)

## Reset everything (removes volumes / data)
reset:
	@echo "⚠️  This will delete all pipeline data. Press Ctrl+C to cancel..."
	@sleep 3
	docker compose down -v --remove-orphans
	@echo "🧹 Reset complete."

## Show running containers and their status
status:
	docker compose ps

## List Kafka topics
kafka-topics:
	docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list

## Show latest rows in the OHLCV table
db-check:
	docker exec postgres psql -U postgres -d stock_db -c \
		"SELECT symbol, bucket_start, open, high, low, close, volume, vwap FROM stock_ohlcv_1m ORDER BY bucket_start DESC LIMIT 20;"

## Show row counts per table
db-stats:
	docker exec postgres psql -U postgres -d stock_db -c \
		"SELECT schemaname, relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC;"

## Clean Docker build cache
clean:
	docker system prune -f
