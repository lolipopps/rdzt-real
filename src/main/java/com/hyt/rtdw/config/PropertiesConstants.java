package com.hyt.rtdw.config;
public class PropertiesConstants {

    public static final String PROPERTIES_FILE_NAME = "application.properties";
    public static final String FEATURE_SQL_PATH = "./sql/";


    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";


    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String KAFKA_GROUP_ID = "group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "group.id";
    public static final String SOURCE_TOPIC = "kafka.source.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";

}
