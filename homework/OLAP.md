```clickhouse
CREATE MATERIALIZED VIEW view_agg_table
            ENGINE = AggregatingMergeTree()
                PARTITION BY toYYYYMM(time)
                ORDER BY (id)
                PRIMARY KEY id
AS
SELECT id, uniqState(code), sumState(v1), time
FROM agg_table_base
GROUP BY id, time;

INSERT INTO TABLE agg_table
SELECT '001', uniqState('C1'), sumState(toUInt32(20)), toDateTime('2021-09-22 15:00:00')
```


