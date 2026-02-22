/* ============================================================
   00_base_1m.sql  (single migration file)
   ============================================================ */

-- 0) Base 1m table (no open_interest, add ingest_time UTC, versioned replacing)
CREATE TABLE IF NOT EXISTS default.dhan_ohlc
(
    sym LowCardinality(String),

    open  Float32,
    high  Float32,
    low   Float32,
    close Float32,

    volume UInt64,

    time DateTime('Asia/Kolkata') CODEC(DoubleDelta, ZSTD(1)),

    ingest_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(ingest_time)
PARTITION BY toYYYYMM(time)
ORDER BY (sym, time);





/* ============================================================
   01_1m_state_and_table.sql
   1m state from base, then 1m table with market-hours filter
   ============================================================ */

CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1m_state
(
  sym LowCardinality(String),
  time DateTime('Asia/Kolkata'),

  open_state  AggregateFunction(argMax, Float32, DateTime64(3, 'Asia/Kolkata')),
  high_state  AggregateFunction(argMax, Float32, DateTime64(3, 'Asia/Kolkata')),
  low_state   AggregateFunction(argMax, Float32, DateTime64(3, 'Asia/Kolkata')),
  close_state AggregateFunction(argMax, Float32, DateTime64(3, 'Asia/Kolkata')),
  vol_state   AggregateFunction(argMax, UInt64,  DateTime64(3, 'Asia/Kolkata'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (sym, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_1m_state
TO default.dhan_ohlc_1m_state
AS
SELECT
  sym,
  time,
  argMaxState(open,  ingest_time) AS open_state,
  argMaxState(high,  ingest_time) AS high_state,
  argMaxState(low,   ingest_time) AS low_state,
  argMaxState(close, ingest_time) AS close_state,
  argMaxState(volume,ingest_time) AS vol_state
FROM default.dhan_ohlc
GROUP BY sym, time;

CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1m
(
  sym LowCardinality(String),
  time DateTime('Asia/Kolkata'),
  open Float32,
  high Float32,
  low Float32,
  close Float32,
  volume UInt64,
  upsert_time DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(upsert_time)
PARTITION BY toYYYYMM(time)
ORDER BY (sym, time);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_1m_state_to_1m
TO default.dhan_ohlc_1m
AS
SELECT
  sym,
  time,
  argMaxMerge(open_state)  AS open,
  argMaxMerge(high_state)  AS high,
  argMaxMerge(low_state)   AS low,
  argMaxMerge(close_state) AS close,
  argMaxMerge(vol_state)   AS volume,
  now64(3, 'UTC')          AS upsert_time
FROM default.dhan_ohlc_1m_state
WHERE
  (toHour(time) > 9 OR (toHour(time) = 9 AND toMinute(time) >= 15))
  AND
  (toHour(time) < 15 OR (toHour(time) = 15 AND toMinute(time) <= 29))
GROUP BY sym, time;


/* ============================================================
   02_rollups_state_and_mvs.sql
   Rollups from 1m table (no FINAL)
   ============================================================ */

-- Source for all rollups below:
--   default.dhan_ohlc_1m


/* ---------------- 2m ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_2m_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_2m_state
TO default.dhan_ohlc_2m_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 2 MINUTE) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 5m ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_5m_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_5m_state
TO default.dhan_ohlc_5m_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 5 MINUTE) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 15m ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_15m_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_15m_state
TO default.dhan_ohlc_15m_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 15 MINUTE) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 30m ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_30m_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_30m_state
TO default.dhan_ohlc_30m_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 30 MINUTE) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 1h ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1h_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_1h_state
TO default.dhan_ohlc_1h_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 1 HOUR) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 2h ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_2h_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_2h_state
TO default.dhan_ohlc_2h_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 2 HOUR) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 4h ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_4h_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_4h_state
TO default.dhan_ohlc_4h_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 4 HOUR) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 1d ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1d_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_1d_state
TO default.dhan_ohlc_1d_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 1 DAY) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 1w (ISO week, starts Monday) ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1w_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_1w_state
TO default.dhan_ohlc_1w_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 1 WEEK) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;


/* ---------------- 1 month ---------------- */
CREATE TABLE IF NOT EXISTS default.dhan_ohlc_1mo_state
(
  sym LowCardinality(String),
  ts DateTime('Asia/Kolkata'),
  open_state  AggregateFunction(argMin, Float32, DateTime('Asia/Kolkata')),
  high_state  AggregateFunction(max, Float32),
  low_state   AggregateFunction(min, Float32),
  close_state AggregateFunction(argMax, Float32, DateTime('Asia/Kolkata')),
  vol_state   AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (sym, ts);

CREATE MATERIALIZED VIEW IF NOT EXISTS default.mv_dhan_ohlc_to_1mo_state
TO default.dhan_ohlc_1mo_state
AS
SELECT
  sym,
  toStartOfInterval(time, INTERVAL 1 MONTH) AS ts,
  argMinState(open, time)  AS open_state,
  maxState(high)           AS high_state,
  minState(low)            AS low_state,
  argMaxState(close, time) AS close_state,
  sumState(volume)         AS vol_state
FROM default.dhan_ohlc_1m
GROUP BY sym, ts;





/* ============================================================
   03_backfill.sql (optional, run once if table already has data)
   ============================================================ */

-- Backfill 1m state (if base already has data)
-- INSERT INTO default.dhan_ohlc_1m_state
-- SELECT
--   sym,
--   time,
--   argMaxState(open,  ingest_time) AS open_state,
--   argMaxState(high,  ingest_time) AS high_state,
--   argMaxState(low,   ingest_time) AS low_state,
--   argMaxState(close, ingest_time) AS close_state,
--   argMaxState(volume,ingest_time) AS vol_state
-- FROM default.dhan_ohlc
-- GROUP BY sym, time;

-- Backfill 1m table from 1m state (market-hours only)
-- INSERT INTO default.dhan_ohlc_1m
-- SELECT
--   sym,
--   time,
--   argMaxMerge(open_state)  AS open,
--   argMaxMerge(high_state)  AS high,
--   argMaxMerge(low_state)   AS low,
--   argMaxMerge(close_state) AS close,
--   argMaxMerge(vol_state)   AS volume,
--   now64(3, 'UTC')          AS upsert_time
-- FROM default.dhan_ohlc_1m_state
-- WHERE
--   (toHour(time) > 9 OR (toHour(time) = 9 AND toMinute(time) >= 15))
--   AND
--   (toHour(time) < 15 OR (toHour(time) = 15 AND toMinute(time) <= 29))
-- GROUP BY sym, time;

-- Backfill rollup states (run each block once as needed)

-- -- 2m
-- INSERT INTO default.dhan_ohlc_2m_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 2 MINUTE) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 5m
-- INSERT INTO default.dhan_ohlc_5m_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 5 MINUTE) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 15m
-- INSERT INTO default.dhan_ohlc_15m_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 15 MINUTE) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 30m
-- INSERT INTO default.dhan_ohlc_30m_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 30 MINUTE) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 1h
-- INSERT INTO default.dhan_ohlc_1h_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 1 HOUR) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 2h
-- INSERT INTO default.dhan_ohlc_2h_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 2 HOUR) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 4h
-- INSERT INTO default.dhan_ohlc_4h_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 4 HOUR) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 1d
-- INSERT INTO default.dhan_ohlc_1d_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 1 DAY) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 1w
-- INSERT INTO default.dhan_ohlc_1w_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 1 WEEK) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

-- -- 1mo
-- INSERT INTO default.dhan_ohlc_1mo_state
-- SELECT
--   sym,
--   toStartOfInterval(time, INTERVAL 1 MONTH) AS ts,
--   argMinState(open, time)  AS open_state,
--   maxState(high)           AS high_state,
--   minState(low)            AS low_state,
--   argMaxState(close, time) AS close_state,
--   sumState(volume)         AS vol_state
-- FROM default.dhan_ohlc_1m
-- GROUP BY sym, ts;

/* ============================================================
   04_views_final.sql
   Finalized (query-time) views over *_state tables
   ============================================================ */

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_2m AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_2m_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_5m AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_5m_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_15m AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_15m_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_30m AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_30m_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_1h AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_1h_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_2h AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_2h_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_4h AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_4h_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_1d AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_1d_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_1w AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_1w_state
GROUP BY sym, ts;

CREATE VIEW IF NOT EXISTS default.dhan_ohlc_1mo AS
SELECT
  sym,
  ts AS time,
  argMinMerge(open_state)  AS open,
  maxMerge(high_state)     AS high,
  minMerge(low_state)      AS low,
  argMaxMerge(close_state) AS close,
  sumMerge(vol_state)      AS volume
FROM default.dhan_ohlc_1mo_state
GROUP BY sym, ts;
-- End of migration file
