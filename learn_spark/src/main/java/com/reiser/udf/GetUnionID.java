package com.reiser.udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * @author: reiserx
 * Date:2021/7/30
 * Des:
 */
public class GetUnionID implements UDF1<String, String> {

    @Override
    public String call(String jsonArray) throws Exception {
        if (jsonArray == null || jsonArray.isEmpty()) {
            return jsonArray;
        }

//        // 2 将string转换为json数组
//        JSONArray actions = new JSONArray(jsonArray);
//
//        // 3 循环一次，取出数组中的一个json，并写出
//        for (int i = 0; i < actions.length(); i++) {
//
//            String[] result = new String[1];
//            result[0] = actions.getString(i);
//
//        }

        return null;
    }
}
