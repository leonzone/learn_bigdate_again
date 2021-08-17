package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/23 10:37
 * @Description:
 *
 *     sum: Computes the sum of all values contained in the RDD.
 *          The approximate version of the function can finish somewhat faster in some scenarios.
 *          However, it trades accuracy for speed.
 *
 **/
object RDD_00_max {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
//        val sparkConf = new SparkConf().setMaster("spark://reiser001:7077").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val rdd1: RDD[Int] = sc.parallelize(List(5, 9, 4, 6, 8, 8, 7, 6))
        
        /*
         * TODO_MA 六大常见聚合操作
         */
        println(rdd1.max())
        println(rdd1.min())
        println(rdd1.count())
        println(rdd1.mean())
        println(rdd1.sum())
        
        rdd1.distinct().foreach(x => print(x + "\t"))
    }
}
