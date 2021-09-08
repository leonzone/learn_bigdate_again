package sparkcore.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object DFDemo {
  def main(args: Array[String]): Unit = {
    val csvPath = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/people.csv"
    val csvPath2 = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/text.csv"

    val path_parquet = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/parquet"
    val path_orc = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/orc"

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("DFDemo")
      //      .config("spark.executor.instances", "10")
      //      .config("spark.executor.memory", "10g")
      .getOrCreate()
    //    支持 spark 语法

    val df = spark.read.json("/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/people.json")
    df.show()

    val df_csv = spark.read.csv(csvPath)
    df_csv.show()


    val df_parquet = spark.read.parquet(path_parquet)
    df_parquet.show()


    val df_orc = spark.read.orc(path_orc)
    df_orc.show()

    import spark.implicits._


    df_orc.map(attributes => {

      "Name: " + attributes(0)

    }).show()


    //    val df_jdbc = spark.read
    //      .format("jdbc")
    //      .option("url", "jdbc:mysql://localhost:3306/crashcourse")
    //      .option("dbtable", "dept_emp")
    //      .option("user", "root")
    //      .option("password", "123456")
    //      .load()
    //
    //    df_jdbc.show()




    val schemaString = "id f1 f2 f3 f4"
    // 通过字符串转换和类型反射生成schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    // 需要将RDD转化为RDD[Row]类型
    val rowRDD = spark.sparkContext.textFile(csvPath2).map(_.split(",")).map(attributes =>
      Row(attributes(0),
        attributes(1),
        attributes(2),
        attributes(3),
        attributes(4).trim)
    )
    // 生成DataFrame
    val df2 = spark.createDataFrame(rowRDD, schema)
    df2.show()

    df.select("age").where("name is not null and age > 10").show()


    df.write.mode(SaveMode.Overwrite).parquet(path_parquet)

    df.write.mode(SaveMode.Append).orc(path_orc)




  }


}
