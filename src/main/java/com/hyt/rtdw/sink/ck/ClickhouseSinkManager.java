package com.hyt.rtdw.sink.ck;

import com.hyt.rtdw.config.ClickhouseSinkCommonParams;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static com.hyt.rtdw.config.ClickhouseSinkCommonParams.MAX_BUFFER_SIZE;
import static com.hyt.rtdw.config.ClickhouseSinkCommonParams.TARGET_TABLE_NAME;

public class ClickhouseSinkManager{
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSinkManager.class);

    private final ClickhouseWriter clickhouseWriter;
    private final ClickhouseSinkScheduledChecker clickhouseSinkScheduledChecker;
    private final ClickhouseSinkCommonParams sinkParams;

    private volatile boolean isClosed = false;

    public ClickhouseSinkManager(Map<String, String> globalParams) {
        sinkParams = new ClickhouseSinkCommonParams(globalParams);
        clickhouseWriter = new ClickhouseWriter(sinkParams);
        clickhouseSinkScheduledChecker = new ClickhouseSinkScheduledChecker(sinkParams);
        logger.info("Build sink writer's manager. params = {}", sinkParams.toString());
    }

    public ClickhouseSinkBuffer buildBuffer(Properties localProperties) {
        String targetTable = localProperties.getProperty(TARGET_TABLE_NAME);
        int maxFlushBufferSize = Integer.valueOf(localProperties.getProperty(MAX_BUFFER_SIZE));

        return buildBuffer(targetTable, maxFlushBufferSize);
    }

    public ClickhouseSinkBuffer buildBuffer(String targetTable, int maxBufferSize) {
        Preconditions.checkNotNull(clickhouseSinkScheduledChecker);
        Preconditions.checkNotNull(clickhouseWriter);

        ClickhouseSinkBuffer clickhouseSinkBuffer = ClickhouseSinkBuffer.Builder
                .aClickhouseSinkBuffer()
                .withTargetTable(targetTable)
                .withMaxFlushBufferSize(maxBufferSize)
                .withTimeoutSec(sinkParams.getTimeout())
                .build(clickhouseWriter);

        clickhouseSinkScheduledChecker.addSinkBuffer(clickhouseSinkBuffer);
        return clickhouseSinkBuffer;
    }

    public boolean isClosed() {
        return isClosed;
    }


    public void close() throws Exception {
        clickhouseWriter.close();
        clickhouseSinkScheduledChecker.close();
        isClosed = true;
    }
}
