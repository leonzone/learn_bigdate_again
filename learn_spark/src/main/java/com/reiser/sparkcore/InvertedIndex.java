package com.reiser.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

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
                    result.add(new Tuple2<>(fileName, word));
                }
                return result.iterator();
            }
        });

        JavaPairRDD<String, List<String>> rdd = word.mapToPair(new PairFunction<Tuple2<String, String>, String, List<String>>() {
            @Override
            public Tuple2<String, List<String>> call(Tuple2<String, String> t) throws Exception {
                List<String> v = new ArrayList<>();
                v.add(t._1);
                return new Tuple2<>(t._2, v);
            }
        });

        JavaPairRDD<String, List<String>> rdd1 = rdd.reduceByKey(new Function2<List<String>, List<String>, List<String>>() {
            @Override
            public List<String> call(List<String> v1, List<String> v2) throws Exception {
                v1.addAll(v2);
                return v1;
            }
        });

        JavaRDD<Tuple2<String, Map<String, Integer>>> rdd2 = rdd1.map(new Function<Tuple2<String, List<String>>, Tuple2<String, Map<String, Integer>>>() {
            @Override
            public Tuple2<String, Map<String, Integer>> call(Tuple2<String, List<String>> t) throws Exception {
                Map<String, Integer> map = new HashMap<>();
                for (String s : t._2) {
                    map.put(s, map.getOrDefault(s, 0) + 1);
                }
                return new Tuple2<>(t._1, map);
            }
        });


        for (Tuple2<String, Map<String, Integer>> next : rdd2.collect()) {
            System.out.println("\"" + next._1 + "\": " + next._2);
        }

        sc.close();
    }
}
