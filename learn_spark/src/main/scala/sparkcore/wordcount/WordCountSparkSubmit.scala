package sparkcore.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-submit
 * maven clean,package
 */
object WordCountSparkSubmit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("My App")
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 2)
      .sortBy(_._2, false)
      .saveAsTextFile(args(1))
  }
}