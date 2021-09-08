package sparkcore.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Auther: 马中华 奈学教育 https://blog.csdn.net/zhongqi2513
 * @Date: 2020/6/17 11:53
 * @Description: A very powerful set of functions that allow grouping up to
 *              3 key-value RDDs together using their keys.
 **/
object RDD_23_coGroup {
    
    def main(args: Array[String]): Unit = {
        
        // 初始化编程入口
        val sparkConf = new SparkConf().setMaster("local").setAppName("RDD_Test")
        val sc = new SparkContext(sparkConf)
        
        val rdd1 = sc.parallelize(List(("huangbo", 1), ("xuzheng", 2), ("shenteng", 3), ("shenteng", 2)))
        val rdd2 = sc.parallelize(List(("huangbo", 33), ("huangbo", 44), ("xuzheng", 11), ("shenteng", 22)))
        val resultRDD = rdd1.cogroup(rdd2)
        resultRDD.foreach(println)

        val result = rdd1.join(rdd2)

        result.foreach(println)


//        (shenteng,(3,22))
//        (shenteng,(2,22))
//        (xuzheng,(2,11))
//        (huangbo,(1,33))
//        (huangbo,(1,44))

//        (shenteng,(CompactBuffer(3, 2),CompactBuffer(22)))
//        (xuzheng,(CompactBuffer(2),CompactBuffer(11)))
//        (huangbo,(CompactBuffer(1),CompactBuffer(33, 44)))
        
//        println("-------------------------------")
//        val rdd3 = sc.parallelize(List(1, 2, 1, 3), 1)
//        val rdd4 = rdd3.map((_, "b"))
//        val rdd5 = rdd3.map((_, "c"))
//        val rdd6 = rdd3.map((_, "d"))
//
//        val resultRDD2: Array[(Int, (Iterable[String], Iterable[String]))] = rdd4.cogroup(rdd5).collect()
//        resultRDD2.foreach(x => println(x))
//
//        println("--------------------------------")
//        val resultRDD3: RDD[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = rdd4.cogroup(rdd5,
//            rdd6)
//        resultRDD3.foreach(x => println(x))
//
//        println("--------------------------------")
//        val rddX = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
//        val rddY = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
//        val resultRDD4: RDD[(Int, (Iterable[String], Iterable[String]))] = rddX.cogroup(rddY)
//        resultRDD4.foreach(println)
    }
    
    def cogroup(sc: SparkContext): Unit = {
        val list1 = List((1, "www"), (2, "bbs"))
        val list2 = List((1, "cnblog"), (2, "cnblog"), (3, "very"))
        val list3 = List((1, "com"), (2, "com"), (3, "good"))
        
        val list1RDD = sc.parallelize(list1)
        val list2RDD = sc.parallelize(list2)
        val list3RDD = sc.parallelize(list3)
        
        list1RDD.cogroup(list2RDD, list3RDD).foreach(tuple =>
            println(tuple._1 + " " + tuple._2._1 + " " + tuple._2._2 + " " + tuple._2._3))
    }
}
