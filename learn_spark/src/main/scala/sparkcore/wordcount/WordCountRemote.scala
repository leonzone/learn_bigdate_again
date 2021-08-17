package sparkcore.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 通过IDEA进行远程调试，主要是将IDEA作为Driver来提交应用程序
 *
 */
object WordCountRemote {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("spark://reiser001:7077")
      .setJars(List("/Users/reiserx/code/hello/learn_bigdata/learn_bigdata_spark/target/learn_bigdata_spark-1.0-SNAPSHOT.jar"))
      .setIfMissing("spark.driver.host","192.168.91.1")
      .setAppName("My App")
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile("hdfs://reiser001:9000/nosort/nosort.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 2)
      .sortBy(_._2, false)
      .saveAsTextFile("hdfs://reiser001:9000/out/out2")
  }
}