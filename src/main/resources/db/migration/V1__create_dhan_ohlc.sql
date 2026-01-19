CREATE TABLE IF NOT EXISTS default.dhan_ohlc
(
    `sym` LowCardinality(String),
    `open` Float32,
    `high` Float32,
    `low` Float32,
    `close` Float32,
    `volume` UInt64,
    `open_interest` UInt64,
    `time` DateTime('Asia/Kolkata') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree
ORDER BY (sym, time);
