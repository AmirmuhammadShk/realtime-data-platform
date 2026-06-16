# Predictiva RealTime Data Platform

This repository contains a production infrastructure design for a realtime market data, news, sentiment, and ML feature streaming platform.


## Architecture

The platform is designed around a streamingfirst architecture.

Market data and news/sentiment events are ingested through dedicated ingestion services. These services validate, normalize, deduplicate, and publish events into Amazon MSK/Kafka using schema controlled event formats.

Kafka acts as the durable event backbone. It allows independent consumers to process the same data stream, supports replay for recovery and backfills, and preserves per asset ordering through asset based partitioning.

Stream processing jobs compute derived candles and realtime features. Aggregated candles and computed features are written back to Kafka and also delivered to serving and storage layers.

The online serving path writes latest features and recent indicators to a lowlatency store such as DynamoDB or Redis. Offline and training pipelines read historical data from S3 backed Iceberg tables. Monitoring systems track service health, Kafka lag, data freshness, missing candles, duplicate events, and feature update latency.

