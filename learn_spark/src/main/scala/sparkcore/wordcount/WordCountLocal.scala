package sparkcore.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 本地模式
 */
object WordCountLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("My App")
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_spark/src/main/resources/test.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)
      .saveAsTextFile("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_spark/src/main/resources/out")


  }
}