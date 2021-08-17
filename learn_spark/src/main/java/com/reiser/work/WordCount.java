package com.reiser.work;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 作者： 马中华：http://blog.csdn.net/zhongqi2513
 * 日期： 2018年04月22日 上午9:27:17
 * <p>
 * 描述： Java版本的WordCount -- 使用lambda表达式
 */
public class WordCount {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage WordCountJavaLambda<input><output>");
            System.exit(1);
        }

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName(WordCount.class.getSimpleName());

        JavaSparkContext jsc = new JavaSparkContext(conf);
        //读取数据
        JavaRDD<String> jrdd = jsc.textFile(args[0]);
        System.out.println(jrdd.toDebugString());
        System.out.println("************************");
        //切割压平
        JavaRDD<String> jrdd2 = jrdd.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
        System.out.println(jrdd2.toDebugString());
        System.out.println("************************");
        //和1组合
        JavaPairRDD<String, Integer> jprdd = jrdd2.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
        System.out.println(jprdd.toDebugString());
        System.out.println("************************");
        //分组聚合
        JavaPairRDD<String, Integer> res = jprdd.reduceByKey((a, b) -> a + b);
        System.out.println(res.toDebugString());
        System.out.println("************************");
        //保存
        res.saveAsTextFile(args[1]);
        res.collect();
        //释放资源
        jsc.close();
    }
}