CREATE TABLE IF NOT EXISTS default.tickers
(
    `symbol` LowCardinality(String),
    `security_id` String,
    `exchange_segment` String,
    `instrument_type` String,
    `last_fetched_time` DateTime,
    `is_active` UInt8 DEFAULT 1,
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree
ORDER BY (symbol);
