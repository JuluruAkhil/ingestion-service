-- 2 Minute
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_2m (
    sym LowCardinality(String),
    open Float32,
    high Float32,
    low Float32,
    close Float32,
    volume UInt64,
    open_interest UInt64,
    time DateTime('Asia/Kolkata')
) ENGINE = ReplacingMergeTree
ORDER BY (sym, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_2m
TO default.dhan_ohlc_2m
AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 2 MINUTE) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 5 Minute
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_5m AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_5m TO default.dhan_ohlc_5m AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 5 MINUTE) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 15 Minute
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_15m AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_15m TO default.dhan_ohlc_15m AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 15 MINUTE) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 30 Minute
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_30m AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_30m TO default.dhan_ohlc_30m AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 30 MINUTE) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 1 Hour
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1h AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_1h TO default.dhan_ohlc_1h AS
SELECT
    sym,
    toStartOfHour(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;


-- 2 Hour
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_2h AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_2h TO default.dhan_ohlc_2h AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 2 HOUR) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;


-- 4 Hour
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_4h AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_4h TO default.dhan_ohlc_4h AS
SELECT
    sym,
    toStartOfInterval(time, INTERVAL 4 HOUR) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 1 Day
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1d AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_1d TO default.dhan_ohlc_1d AS
SELECT
    sym,
    toStartOfDay(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 1 Week
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1w AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_1w TO default.dhan_ohlc_1w AS
SELECT
    sym,
    toStartOfWeek(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

-- 1 Month
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1M AS default.dhan_ohlc_2m;
CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_1M TO default.dhan_ohlc_1M AS
SELECT
    sym,
    toStartOfMonth(time) AS time,
    argMin(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    argMax(close, time) AS close,
    sum(volume) AS volume,
    argMax(open_interest, time) AS open_interest
FROM default.dhan_ohlc
GROUP BY sym, time;

