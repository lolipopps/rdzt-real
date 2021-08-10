package com.hyt.rtdw.config;


import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.Map;
public class ClickhouseSinkCommonParams {

    public static final String TARGET_TABLE_NAME = "clickhouse.sink.target-table";
    public static final String MAX_BUFFER_SIZE = "clickhouse.sink.max-buffer-size";

    public static final String NUM_WRITERS = "clickhouse.sink.num-writers";
    public static final String QUEUE_MAX_CAPACITY = "clickhouse.sink.queue-max-capacity";
    public static final String TIMEOUT_SEC = "clickhouse.sink.timeout-sec";
    public static final String NUM_RETRIES = "clickhouse.sink.retries";
    public static final String FAILED_RECORDS_PATH = "clickhouse.sink.failed-records-path";

    private final ClickhouseClusterSettings clickhouseClusterSettings;

    private final String failedRecordsPath;
    private final int numWriters;
    private final int queueMaxCapacity;

    private final int timeout;
    private final int maxRetries;

    public ClickhouseSinkCommonParams(Map<String, String> params) {
        this.clickhouseClusterSettings = new ClickhouseClusterSettings(params);
        this.numWriters = Integer.valueOf(params.get(NUM_WRITERS));
        this.queueMaxCapacity = Integer.valueOf(params.get(QUEUE_MAX_CAPACITY));
        this.maxRetries = Integer.valueOf(params.get(NUM_RETRIES));
        this.timeout = Integer.valueOf(params.get(TIMEOUT_SEC));
        this.failedRecordsPath = params.get(FAILED_RECORDS_PATH);

        Preconditions.checkNotNull(failedRecordsPath);
        Preconditions.checkArgument(queueMaxCapacity > 0);
        Preconditions.checkArgument(numWriters > 0);
        Preconditions.checkArgument(timeout > 0);
        Preconditions.checkArgument(maxRetries > 0);
    }

    public int getNumWriters() {
        return numWriters;
    }

    public int getQueueMaxCapacity() {
        return queueMaxCapacity;
    }

    public ClickhouseClusterSettings getClickhouseClusterSettings() {
        return clickhouseClusterSettings;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public String getFailedRecordsPath() {
        return failedRecordsPath;
    }

    @Override
    public String toString() {
        return "ClickhouseSinkCommonParams{" +
                "clickhouseClusterSettings=" + clickhouseClusterSettings +
                ", failedRecordsPath='" + failedRecordsPath + '\'' +
                ", numWriters=" + numWriters +
                ", queueMaxCapacity=" + queueMaxCapacity +
                ", timeout=" + timeout +
                ", maxRetries=" + maxRetries +
                '}';
    }
}
