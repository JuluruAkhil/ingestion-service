# DhanHQ Ingestion Service

Fault-tolerant, high-concurrency data ingestion service for DhanHQ built with Spring Boot 4 and Java 21.

## What It Does
- Pulls market data from DhanHQ and stores it in ClickHouse.
- Uses Flyway migrations to create tables, aggregations, and seed data.
- Runs concurrent fetches with resilient retry and rate limiting.

## Prerequisites
1. Java 21
2. ClickHouse running and reachable
3. Maven Wrapper (`./mvnw`) is included

## Configuration
Copy `.env.example` to `.env` (or export variables in your shell) and fill in values.

Required variables:
- `ACCESS_TOKEN`
- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`

Optional variables:
- `INGESTION_CRON` (default: `0 */15 * * * *`)
- `DHAN_INFLIGHT_LIMIT` (default: `10`)

## Database Setup
Flyway runs on startup and creates:
- `dhan_ohlc` (main data table)
- `tickers` (cursor tracking)
- Aggregation tables and views (2m .. 1M)

## How to Run
1. Build:
   ```bash
   ./mvnw -DskipTests package
   ```
2. Run:
   ```bash
   java -jar target/ingestion-service-0.0.1-SNAPSHOT.jar
   ```

## Key Features
- Bellwether check to skip cycles when no new data
- Cursor-based sync for crash recovery
- High concurrency with rate limiting

## Troubleshooting
- Check console logs first
- If a symbol is stuck, adjust `last_fetched_time` in `tickers`
