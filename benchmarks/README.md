# Reproducible performance checks

Start the complete stack in simulation mode, wait until `/api/health` reports all
services as healthy, then run:

```bash
python benchmarks/run_dashboard_load.py \
  --url http://localhost:5000/api/candles/AAPL?limit=200 \
  --requests 1000 \
  --concurrency 25
```

The command reports success/error counts, throughput, and mean/p50/p95/p99 HTTP
latency as JSON. Record the machine specification, Git commit, command, data mode,
and timestamp beside any published result. No performance number should be claimed
without saving that evidence.

Pipeline metrics are available in Prometheus at <http://localhost:9090>. Useful
series include:

- `stock_pipeline_ticks_published_total`
- `stock_pipeline_raw_rows_written_total`
- `stock_pipeline_candle_rows_written_total`
- `stock_pipeline_postgres_write_seconds`
- `stock_pipeline_events_per_second`
- `stock_pipeline_latest_event_age_seconds`
- Kafka exporter topic and consumer-group metrics

This repository intentionally does not contain invented benchmark results.
