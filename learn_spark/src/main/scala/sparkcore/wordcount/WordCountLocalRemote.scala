package sparkcore.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 本地模式,连接 HDFS
 * 本地Spark程序调试需要使用local提交模式，即将本机当做运行环境，Master和Worker都为本机。运行时直接加断点调试即可
 */
object WordCountLocalRemote {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("My App")
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile("hdfs://reiser001:9000/nosort/nosort.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 2)
      .sortBy(_._2, false)
      .saveAsTextFile("hdfs://reiser001:9000/out/out1")


  }
}