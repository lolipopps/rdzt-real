package com.hyt.rtdw.util;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;

public class JsonUtil {


    /**
     * 获取表的元数据结构
     *
     * @return HashMap<String, HashMap < String, String>>
     */
    public static Map<String, String> loadJson(String dimFilePath) {
        try {
            String dim = FileUtil.readJsonFile(dimFilePath);
            JSONObject obj = JSONObject.parseObject(dim);
            HashMap<String, String> res = new HashMap<>();
            for (String key : obj.keySet()) {
                String record = obj.getJSONObject(key).toJSONString();
                res.put(key, record);
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static void main(String[] args) {
        Map<String, String> res = loadJson("/Users/yth/code/java/rdzt-real/src/main/resources/data/ods_showing_showings_rt_si.json");
        for(Map.Entry re:res.entrySet()){
            System.out.println(re.getKey());

            System.out.println(re.getValue());
        }


    }
}
