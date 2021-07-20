package com.hyt.rtdw.config;

/**
 * 常量内容
 * @author zhangzhiyuan
 * @date 2019/10/23
 */
public interface Constant {
    String brokers = "kafka01-test.lianjia.com:9092,kafka02-test.lianjia.com:9092,kafka03-test.lianjia.com:9092";
    String source_topics = "bigdata-hermes-sqlv2-source-v2";
    String sink_topic = "bigdata-hermes-sqlv2-sink-v2";
}
