package com.reiser.sparksql.car

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WebUI {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DFDemo")
      //      .config("spark.executor.instances", "10")
      //      .config("spark.executor.memory", "10g")
      .getOrCreate()

    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g

    val rootPath: String = "/user/student04/xuzeze/car"

    // 申请者数据
    val hdfs_path_apply = s"$rootPath/apply"
    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    // 创建Cache并触发Cache计算
    applyNumbersDF.cache.count()

    // 中签者数据
    val hdfs_path_lucky = s"$rootPath/lucky"
    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    //创建Cache并触发Cache计算
    luckyDogsDF.cache.count()

    val result05_01 = applyNumbersDF
      // 按照carNum做关联
      .join(luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum")
      // 取最大倍率
      .agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier")
      // 按照倍率做分组计数
      .agg(count(lit(1)).alias("cnt"))
      // 按照倍率排序
      .orderBy("multiplier")

    result05_01.write.mode("Overwrite").format("csv").save(s"${rootPath}/results/result05_01")


  }

}
