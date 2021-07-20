package com.hyt.rtdw.util;

import com.hyt.rtdw.config.PropertiesConstants;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TableUtil {


    public void getSql(StreamTableEnvironment tableEnv, String fileName) throws IOException {
        InputStream inputStream = null;
        inputStream = ExecutionEnvUtil.class.getClassLoader().getResourceAsStream(PropertiesConstants.FEATURE_SQL_PATH + fileName);
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }
        String sql = result.toString("UTF-8");
        tableEnv.executeSql(sql);
    }

}
