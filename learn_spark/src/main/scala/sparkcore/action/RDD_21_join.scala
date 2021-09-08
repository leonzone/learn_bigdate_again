package sparkcore.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:52
 * @Description: Performs an inner join using two key-value RDDs.
 *              Please note that the keys must be generally comparable to make this work.
 **/
object RDD_21_join {
    
    def main(args: Array[String]): Unit = {
    
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
    
        val data1 = List((1, "sunli"), (2, "wuqilong"), (3, "huangxiaoming"))
        val data2 = List((1, "dengchao"), (3, "baby"), (2, "liushishi"))
        val rdd1: RDD[(Int, String)] = sc.parallelize(data1)
        val rdd2: RDD[(Int, String)] = sc.parallelize(data2)
    
        // 注意：只有key-value类型的RDD才能进行join
        val data3 = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
        
        // 求 join
        val resultRDD = rdd1.join(rdd2)
    
        // 打印RDD
        resultRDD.foreach(println)
    }
    
    def join(sc: SparkContext): Unit = {
        val list1 = List((1, "东方不败"), (2, "令狐冲"), (3, "林平之"))
        val list2 = List((1, 99), (2, 98), (3, 97))
        val list1RDD:RDD[(Int, String)] = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        val joinRDD = list1RDD.join(list2RDD)
        joinRDD.foreach(t => println("学号:" + t._1 + " 姓名:" + t._2._1 + " 成绩:" + t._2._2))
    }
}
