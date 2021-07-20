package com.hyt.rtdw.util;

import com.hyt.rtdw.config.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: hyt
 * @License: (C) Copyright 2020-2020, xxx Corporation Limited.
 * @Contact: xxx@xxx.com
 * @Version: 1.0
 * @Description: flink 执行的全局参数加载
 */
@Slf4j
public class ExecutionEnvUtil {


    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    public static ParameterTool createParameterTool(final String[] args) {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromArgs(args))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    static ParameterTool createParameterTool() {
        try {

            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // flink 执行环境配置
    public static StreamExecutionEnvironment prepare() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARAMETER_TOOL.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        env.getConfig().setGlobalJobParameters(PARAMETER_TOOL); // make parameters available in the web interface
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        if (PARAMETER_TOOL.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(PARAMETER_TOOL.getInt(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 1000)); // create a checkpoint every 5 seconds
            env.getCheckpointConfig().enableExternalizedCheckpoints(
                    CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        }
        return env;
    }


    // flin 执行环境配置
    public static ExecutionEnvironment prepareBatch() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARAMETER_TOOL.getInt(PropertiesConstants.STREAM_PARALLELISM, 5));
        env.getConfig().setGlobalJobParameters(PARAMETER_TOOL); // make parameters available in the web interface
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        return env;
    }



    // 环境参数
    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
