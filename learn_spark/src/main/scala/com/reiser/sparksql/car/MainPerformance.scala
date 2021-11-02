package com.reiser.sparksql.car

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object MainPerformance {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DFDemo")
      //      .config("spark.executor.instances", "10")
      //      .config("spark.executor.memory", "10g")
      .getOrCreate()

    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g

    val rootPath: String = "/user/student04/xuzeze/car"

    //案例 1：人数统计
    // 申请者数据（因为倍率的原因，每一期，同一个人，可能有多个号码）
    val hdfs_path_apply = s"$rootPath/apply"
    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    // FIXME 优化：cache 优化后面的查询
    applyNumbersDF.cache()
    applyNumbersDF.count
    // 中签者数据
    val hdfs_path_lucky = s"$rootPath/lucky"
    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    luckyDogsDF.count

    //摇号批次去重
    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct
    // 使用了 applyNumbersDF.cache() 后，查询速度提升，因为从之前的磁盘读取数据变成内存读取
    applyDistinctDF.count


    //案例 2：摇号次数分布
    // FIXME 优化方案：无 - 59s+33s
    val result02_01 = applyDistinctDF
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")
    result02_01.write.format("csv").save("/user/student04/xuzeze/result02_01")
    // FIXME 优化方案一：配置项调整
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.shuffle.file.buffer=48k --conf spark.reducer.maxSizeInFlight
    // spark-shell  --conf spark.shuffle.file.buffer=48k --conf spark.reducer.maxSizeInFlight=72m

    // FIXME 优化方案二：修改并行度

    def sizeNew(func: => DataFrame, spark: => SparkSession): String = {
      val result = func
      val lp = result.queryExecution.logical
      val size = spark.sessionState.executePlan(lp).optimizedPlan.stats.sizeInBytes
      "Estimated size: " + size/1024 + "KB"
    }
    // spark-shell  --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf  spark.sql.adaptive.advisoryPartitionSizeInBytes=200MB --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=6

    // FIXME 优化方案三：加 cache - 18s

    //    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct
    applyDistinctDF.cache
    applyDistinctDF.count

    val result02_01_2 = applyDistinctDF
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")


    result02_01_2.write.format("csv").save("/user/student04/xuzeze/result02_01_2")


    val result02_02 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    result02_02.write.format("csv").save("/user/student04/xuzeze/result02_02")

    sizeNew(luckyDogsDF,spark)

    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.autoBroadcastJoinThreshold=20971520

    val result02_02_1 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    result02_02_1.write.format("csv").save("/user/student04/xuzeze/result02_02_1")
  }


}
