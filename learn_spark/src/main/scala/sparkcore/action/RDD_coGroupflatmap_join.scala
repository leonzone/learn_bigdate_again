package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:53
 * @Description: A very powerful set of functions that allow grouping up to
 *              3 key-value RDDs together using their keys.
 **/
object RDD_coGroupflatmap_join {

    def main(args: Array[String]): Unit = {

        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)

        val rdd1 = sc.parallelize(List(("huangbo", 1), ("xuzheng", 2), ("shenteng", 3), ("shenteng", 2)))
        val rdd2 = sc.parallelize(List(("huangbo", 33), ("huangbo", 44), ("xuzheng", 11), ("shenteng", 22)))
        //        (shenteng,(CompactBuffer(3, 2),CompactBuffer(22)))
        //        (xuzheng,(CompactBuffer(2),CompactBuffer(11)))
        //        (huangbo,(CompactBuffer(1),CompactBuffer(33, 44)))
        val rdd3 = rdd1.cogroup(rdd2)
        val resultRDD = rdd3.flatMapValues(pair => for (v <- pair._1.iterator; w <- pair._2.iterator) yield (v, w))
        resultRDD.foreach(println)

//        (shenteng,(3,22))
//        (shenteng,(2,22))
//        (xuzheng,(2,11))
//        (huangbo,(1,33))
//        (huangbo,(1,44))


        
    }
}
