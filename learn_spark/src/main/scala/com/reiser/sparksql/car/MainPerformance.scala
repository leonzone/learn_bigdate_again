package com.reiser.sparksql.car

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainPerformance {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DFDemo")
      //      .config("spark.executor.instances", "10")
      //      .config("spark.executor.memory", "10g")
      .getOrCreate()

    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.dynamicAllocation.enabled=true

    val rootPath: String = "/user/student04/xuzeze/car"

    //案例 1：人数统计
    // 申请者数据（因为倍率的原因，每一期，同一个人，可能有多个号码）
    val hdfs_path_apply = s"$rootPath/apply"
    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    // FIXME 优化：cache 优化后面的查询buffer
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
    applyDistinctDF.unpersist()
    applyNumbersDF.unpersist()


    //案例 1：摇号次数分布
    // FIXME 优化方案：无 - 59s+33s
    val result01_01 = applyDistinctDF
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")
    result01_01.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result01_01")
    // FIXME 优化方案一：配置项调整
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.shuffle.file.buffer=48k --conf spark.reducer.maxSizeInFlight
    // spark-shell  --conf spark.shuffle.file.buffer=48k --conf spark.reducer.maxSizeInFlight=72m

    // FIXME 优化方案二：修改并行度

    val result01_02 = applyDistinctDF
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")
    result01_02.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result01_02")

    def sizeNew(func: => DataFrame, spark: => SparkSession): String = {
      val result = func
      val lp = result.queryExecution.logical
      val size = spark.sessionState.executePlan(lp).optimizedPlan.stats.sizeInBytes
      "Estimated size: " + size / 1024 + "KB"
    }


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

    // join 场景
    val result02_02 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    result02_02.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result02_02")

    sizeNew(luckyDogsDF, spark)

    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.autoBroadcastJoinThreshold=20971520

    val result02_02_1 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    result02_02_1.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result02_02_1")


    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.autoBroadcastJoinThreshold=20971520

    // 案例 3 的性能调优：中签率的变化趋势
    // 统计每批次申请者的人数
    val apply_denominator = applyDistinctDF
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("denominator"))

    // 统计每批次中签者的人数
    val lucky_molecule = luckyDogsDF
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("molecule"))


    Case4(spark)


  }
  {

  }


  {
    //shuffle 优化
    //spark-shell  --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.shuffle.file.buffer=48k --conf spark.reducer.maxSizeInFlight=72m

  }
  {
    // 分区合并
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.adaptive.enabled=false
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true --conf  spark.sql.adaptive.advisoryPartitionSizeInBytes=200MB --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=6

    //    val rootPath: String = "/user/student04/xuzeze/car"
    //    val hdfs_path_apply = s"$rootPath/apply"
    //    val hdfs_path_lucky = s"$rootPath/lucky"
    //    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    //    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    //    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct

  }

  //案例 4 的性能调优：中签率局部洞察
  private def Case4(spark: SparkSession) = {
    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 8g --conf spark.sql.autoBroadcastJoinThreshold=209715200

    val rootPath: String = "/user/student04/xuzeze/car"

    //案例 1：人数统计
    // 申请者数据（因为倍率的原因，每一期，同一个人，可能有多个号码）
    val hdfs_path_apply = s"$rootPath/apply"
    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    // FIXME 优化：cache 优化后面的查询
    applyNumbersDF.cache()
    // 中签者数据
    val hdfs_path_lucky = s"$rootPath/lucky"
    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct
    // 使用了 applyNumbersDF.cache() 后，查询速度提升，因为从之前的磁盘读取数据变成内存读取
    applyDistinctDF.cache()

    // 统计每批次申请者的人数
    val apply_denominator = applyDistinctDF.groupBy(col("batchNum")).agg(count(lit(1)).alias("denominator"))

    //    // 筛选出2018年的中签数据，并按照批次统计中签人数
    val lucky_molecule_2018 = luckyDogsDF.filter(col("batchNum").like("2018%")).groupBy(col("batchNum")).agg(count(lit(1)).alias("molecule"))
    //
    //
    //    // 通过与筛选出的中签数据按照批次做关联，计算每期的中签率
    val result04 = apply_denominator
      .join(lucky_molecule_2018, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule") / col("denominator"), 5))
      .orderBy("batchNum")
    //
    result04.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result04")


    val result04_01 = apply_denominator
      .join(lucky_molecule_2018, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule") / col("denominator"), 5))
      .orderBy("batchNum")
    //
    result04_01.write.mode("overwrite").format("csv").save("/user/student04/xuzeze/result04_01")

  }

  {

    //    // 统计每批次申请者的人数
    //    val apply_denominator = applyDistinctDF
    //      .groupBy(col("batchNum"))
    //      .agg(count(lit(1)).alias("denominator"))
    //
    //    // 统计每批次中签者的人数
    //    val lucky_molecule = luckyDogsDF
    //      .groupBy(col("batchNum"))
    //      .agg(count(lit(1)).alias("molecule"))
    //
    //    val result03 = apply_denominator
    //      .join(lucky_molecule, Seq("batchNum"), "inner")
    //      .withColumn("ratio", round(lucky_molecule.col("molecule")/col("denominator"), 5))
    //      .orderBy("batchNum")
    //
    //    result03.write.format("csv").save("_")
    //  }

    //   def Case6(spark: SparkSession) = {
    //
    //    // spark-shell --driver-cores 1 --driver-memory 5g --executor-cores 3 --executor-memory 3g
    //
    //    // HDFS根目录地址
    //    val rootPath: String = "/user/student04/xuzeze/car"
    //
    //    // 申请者数据（因为倍率的原因，每一期，同一个人，可能有多个号码）
    //    val hdfs_path_apply = s"$rootPath/apply"
    //    val applyNumbersDF = spark.read.parquet(hdfs_path_apply)
    //    // FIXME 优化：cache 优化后面的查询
    //    applyNumbersDF.cache.count
    //    // 中签者数据
    //    val hdfs_path_lucky = s"$rootPath/lucky"
    //    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    //    luckyDogsDF.cache.count
    //
    //
    //    val result05_01 = applyNumbersDF
    //      // 按照carNum做关联
    //      .join(luckyDogsDF.filter(col("batchNum") >= "201601").select("carNum"), Seq("carNum"), "inner")
    //      .groupBy(col("batchNum"), col("carNum"))
    //      .agg(count(lit(1)).alias("multiplier"))
    //      .groupBy("carNum")
    //      // 取最大倍率
    //      .agg(max("multiplier").alias("multiplier"))
    //      .groupBy("multiplier")
    //      // 按照倍率做分组计数
    //      .agg(count(lit(1)).alias("cnt"))
    //      // 按照倍率排序
    //      .orderBy("multiplier")
    //
    //    result05_01.write.mode("Overwrite").format("csv").save(s"${rootPath}/results/result05_01")
    //
    //  }
  }
}
