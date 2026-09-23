# GitHub repository settings

These settings require repository-owner access and cannot be encoded in the source tree.

- Recommended repository name: `real-time-stock-data-pipeline`
- About: `Kafka and PySpark pipeline that converts live or simulated market ticks into event-time one-minute OHLCV candles served by a Flask dashboard.`
- Topics: `apache-kafka`, `pyspark`, `postgresql`, `flask`, `stream-processing`, `data-engineering`, `prometheus`, `docker`

After CI is green and the end-to-end workflow passes, create a `v1.0.0` release from
the verified commit. Do not tag a release before that point.
