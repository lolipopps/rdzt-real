-- register a MySQL table 'users' in Flink SQL
CREATE TABLE MyUserTable
(
    id     BIGINT,
    name   STRING,
    age    INT,
    status BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
      'connector' = 'jdbc',
      'username' = 'root',
      'password' = 'hu1234tai',
      'driver' = 'com.mysql.jdbc.Driver',
      'url' = 'jdbc:mysql://172.18.1.6:3306/game_platform?useSSL=false&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8',
      'table-name' = 'users'
);
