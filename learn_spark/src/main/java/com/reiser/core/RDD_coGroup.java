package com.reiser.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * @author: reiserx
 * Date:2021/8/30
 * Des:
 */
public class RDD_coGroup {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName(RDD_coGroup.class.getSimpleName()).setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        JavaPairRDD<String, Integer> rdd1 = sc.parallelize(Arrays.asList(new Tuple2<>("huangbo", 1), new Tuple2<>("xuzheng", 2), new Tuple2<>("shenteng", 3), new Tuple2<>("shenteng", 2))).mapToPair(t -> t);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelize(Arrays.asList(new Tuple2<>("huangbo", 33), new Tuple2<>("huangbo", 44), new Tuple2<>("xuzheng", 11), new Tuple2<>("shenteng", 22))).mapToPair(t -> t);

//        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = rdd1.cogroup(rdd2);


    }
}
