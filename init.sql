CREATE MATERIALIZED VIEW candlesticks_pepe_usdt_10min
            WITH (timescaledb.continuous) AS
SELECT
    time_bucket('10 minutes', time) AS bucket,
    FIRST(open, time) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close, time) AS close,
    SUM(volume) AS volume
FROM candlesticks_pepe_usdt
GROUP BY bucket;

