package com.reiser.sparksql.car

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
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
    applyNumbersDF.count
    // 中签者数据
    val hdfs_path_lucky = s"$rootPath/lucky"
    val luckyDogsDF = spark.read.parquet(hdfs_path_lucky)
    luckyDogsDF.count

    //摇号批次去重
    val applyDistinctDF = applyNumbersDF.select("batchNum", "carNum").distinct
    applyDistinctDF.count

//    applyNumbersDF.createOrReplaceTempView("tb_apply")
//    luckyDogsDF.createOrReplaceTempView("tb_lucky")
//    applyDistinctDF.createOrReplaceTempView("tb_apply_distinct")


    //案例 2：摇号次数分布
    val result02_01 = applyDistinctDF
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    val result02_02 = applyDistinctDF
      .join(luckyDogsDF.select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("carNum"))
      .agg(count(lit(1)).alias("x_axis"))
      .groupBy(col("x_axis"))
      .agg(count(lit(1)).alias("y_axis"))
      .orderBy("x_axis")

    //案例 3：中签率的变化趋势

    // 统计每批次申请者的人数
    val apply_denominator = applyDistinctDF
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("denominator"))

    // 统计每批次中签者的人数
    val lucky_molecule = luckyDogsDF
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("molecule"))

    val result03 = apply_denominator
      .join(lucky_molecule, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule") / col("denominator"), 5))
      .orderBy("batchNum")

    result03.write.format("csv").save("_")


    // 筛选出2018年的中签数据，并按照批次统计中签人数
    val lucky_molecule_2018 = luckyDogsDF
      .filter(col("batchNum").like("2018%"))
      .groupBy(col("batchNum"))
      .agg(count(lit(1)).alias("molecule"))

    // 通过与筛选出的中签数据按照批次做关联，计算每期的中签率
    val result04 = apply_denominator
      .join(lucky_molecule_2018, Seq("batchNum"), "inner")
      .withColumn("ratio", round(col("molecule") / col("denominator"), 5))
      .orderBy("batchNum")

    result04.write.format("csv").save("_")


    //    案例 5：倍率分析

    val result05_01 = applyNumbersDF
      .join(luckyDogsDF.filter(col("batchNum") >= "201601")
        .select("carNum"), Seq("carNum"), "inner")
      .groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier")
      .agg(count(lit(1)).alias("cnt"))
      .orderBy("multiplier")

    result05_01.write.format("csv").save("_")


    // Step01: 过滤出2016-2019申请者数据，统计出每个申请者在每一期内的倍率，并在所有批次中选取最大的倍率作为申请者的最终倍率，最终算出各个倍率下的申请人数
    val apply_multiplier_2016_2019 = applyNumbersDF
      .filter(col("batchNum") >= "201601")
      .groupBy(col("batchNum"), col("carNum"))
      .agg(count(lit(1)).alias("multiplier"))
      .groupBy("carNum")
      .agg(max("multiplier").alias("multiplier"))
      .groupBy("multiplier")
      .agg(count(lit(1)).alias("apply_cnt"))

    // Step02: 将各个倍率下的申请人数与各个倍率下的中签人数左关联，并求出各个倍率下的中签率
    val result05_02 = apply_multiplier_2016_2019
      .join(result05_01.withColumnRenamed("cnt", "lucy_cnt"), Seq("multiplier"), "left")
      .na.fill(0)
      .withColumn("ratio", round(col("lucy_cnt") / col("apply_cnt"), 5))
      .orderBy("multiplier")

    result05_02.write.format("csv").save("_")
  }


}
