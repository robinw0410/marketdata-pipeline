WITH b AS (
  SELECT open_time, lag(open_time) OVER (ORDER BY open_time) AS prev
  FROM read_parquet('/opt/marketdata/data/klines/**/*.parquet', hive_partitioning=1)
  WHERE symbol = 'BTCUSDT'
)
SELECT prev AS luecke_ab, open_time AS weiter_ab,
       datediff('minute', prev, open_time) - 1 AS fehlende_bars
FROM b
WHERE datediff('minute', prev, open_time) > 1
ORDER BY fehlende_bars DESC;
