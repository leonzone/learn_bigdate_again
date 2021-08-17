package com.reiser.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author: reiserx
 * Date:2021/8/17
 * Des: 使用 spark 实现倒排索引
 */
public class InvertedIndex {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(InvertedIndex.class.getSimpleName());

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaRDD<Tuple2<String, String>> word = lines.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterator<Tuple2<String, String>> call(String s) throws Exception {
                String[] words = s.split("\\.");
                String fileName = words[0];
                String line = words[1].replace("\"", "");
                ArrayList<Tuple2<String, String>> result = new ArrayList<>();
                for (String word : line.split(" ")) {
                    result.add(new Tuple2<>(word, fileName));
                }
                return result.iterator();
            }
        });

        JavaPairRDD<String, String> rdd = word.mapToPair((PairFunction<Tuple2<String, String>, String, String>) t -> new Tuple2<>(t._1, t._2));

        JavaPairRDD<String, String> rdd1 = rdd.reduceByKey((Function2<String, String, String>) (v1, v2) -> v1 + "|" + v2);


        JavaRDD<Tuple2<String, Map<String, Integer>>> rdd2 = rdd1.map(t -> {
            Map<String, Integer> map = new HashMap<>();
            for (String s : t._2.split("\\|")) {
                map.put(s, map.getOrDefault(s, 0) + 1);
            }
            return new Tuple2<>(t._1, map);
        });
        for (Tuple2<String, Map<String, Integer>> next : rdd2.collect()) {
            System.out.println("\"" + next._1 + "\": " + next._2);
        }
        sc.close();
    }
}
