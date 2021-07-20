CREATE TABLE orders
(
    order_id   STRING,
    item       STRING,
    currency   STRING,
    amount     DOUBLE,
    order_time TIMESTAMP(3),
    proc_time as PROCTIME(),
    amount_kg as amount * 1000,
    ts as order_time + INTERVAL '1' SECOND,
    WATERMARK FOR order_time AS order_time
) WITH (
      'connector' = 'kafka',
      'topic' = 'order_table',
      'connector.version' = 'universal',
      'property-version' = 'universal',
      'properties.zookeeper.connect' = 'localhost:2181',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'tttt',
      'connector.startup-mode' = 'latest-offset',
      'format' = 'json',
      'format.derive-schema' = 'true'
)