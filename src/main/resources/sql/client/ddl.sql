CREATE TABLE orders
(
    order_id   STRING,
    item       STRING,
    currency   STRING,
    amount     DOUBLE,
    ts_time    DOUBLE,
    order_time TIMESTAMP(3),
    proc_time as PROCTIME(),
    amount_kg as amount * 1000
) with ('connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.properties.group.id' = 'g2.group1',
      'connector.properties.bootstrap.servers' = '172.18.1.21:9092',
      'connector.properties.zookeeper.connect' = '172.18.1.21:2181',
      'connector.topic' = 'order_table',
      'connector.startup-mode' = 'latest-offset',
      'format.type' = 'json'
      );

CREATE TABLE orders
(
    order_id   STRING,
    item       STRING,
    currency   STRING,
    amount     DOUBLE,
    ts_time    DOUBLE,
    order_time TIMESTAMP(3),
    proc_time as PROCTIME(),
    amount_kg as amount * 1000
) with ('connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.properties.group.id' = 'g2.group1',
      'connector.properties.bootstrap.servers' = '172.18.1.21:9092',
      'connector.properties.zookeeper.connect' = '172.18.1.21:2181',
      'connector.topic' = 'order_table',
      'connector.startup-mode' = 'latest-offset',
      'format.type' = 'json'
      );

CREATE TABLE orders_detail
(
    order_id   STRING,
    item       STRING,
    state   STRING,
    create_time TIMESTAMP(3)
) with ('connector.type' = 'kafka',
      'connector.version' = 'universal',
      'connector.properties.group.id' = 'g2.group1',
      'connector.properties.bootstrap.servers' = '172.18.1.21:9092',
      'connector.properties.zookeeper.connect' = '172.18.1.21:2181',
      'connector.topic' = 'orders_detail',
      'connector.startup-mode' = 'latest-offset',
      'format.type' = 'json',
      'format.derive-schema' = 'true'
      );

CREATE TABLE currency
(
    country       STRING,
    currency      STRING,
    rate          INT,
    currency_time TIMESTAMP(3)
) WITH (
      'connector.type' = 'kafka',
      'connector.topic' = 'currency_table',
      'connector.version' = 'universal',
      'connector.properties.zookeeper.connect' = 'localhost:2181',
      'connector.properties.bootstrap.servers' = 'localhost:9092',
      'connector.properties.group.id' = 'testGroup',
      'connector.startup-mode' = 'latest-offset',
      'format.type' = 'json',
      'format.derive-schema' = 'true'
      );