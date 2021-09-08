package com.reiser.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2021/9/1
 * Des:
 */
public class DFDemo {
    static String csvPath = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/people.csv";
    static String csvPath2 = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/text.csv";

    static String path_parquet = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/parquet";
    static String path_orc = "/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/orc";

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("DFDemo")
                //      .config("spark.executor.instances", "10")
                //      .config("spark.executor.memory", "10g")
                .getOrCreate();

        //读取文件
//        Dataset<Row> df = readFile(spark);

        //读取JDBC
//        readJDBC(spark);


        //自定义Schema
//        readSchema(spark);
//
//        select(df);
////
////        df.select("age").where("name is not null and age > 10").show()
////
//        //查询
//        writeFile(df);
//
//
//        createDataSet(spark);

        udf(spark);

        spark.close();

    }

    private static void udf(SparkSession spark) {



        spark.udf().register("oneArgFilter", new UDF1<Long, Boolean>() {
            @Override
            public Boolean call(Long x) {
                return  x > 5;
            }
        }, DataTypes.BooleanType);
        spark.range(1, 10).createOrReplaceTempView("test");
        spark.sql("SELECT * FROM test WHERE oneArgFilter(id)").show();
    }

    private static void createDataSet(SparkSession spark) {

        //转换 DF 为 DataSet
        Dataset<Row> dfSG = spark.read().json("/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/student_grade.json");
        Dataset<StudentGrade> agradeDS = dfSG.as(Encoders.bean(StudentGrade.class));
        agradeDS.show();
    }

    private static void select(Dataset<Row> df) {
        // DataFrame 的查询风格分为两种：算子风格和 SQL 风格
//        算子风格
        df.groupByKey(new MapFunction<Row, String>() {
            @Override
            public String call(Row value) throws Exception {
                return null;
            }
        }, Encoders.STRING());
    }

    private static void writeFile(Dataset<Row> df) {
        df.write().mode(SaveMode.Overwrite).parquet(path_parquet);
        df.write().mode(SaveMode.Append).orc(path_orc);
    }

    private static void readSchema(SparkSession spark) {
        String schemaString = "id f1 f2 f3 f4";
        // 通过字符串转换和类型反射生成schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField filed = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(filed);
        }

        StructType schema = DataTypes.createStructType(fields);

        // 需要将RDD转化为RDD[Row]类型
        JavaRDD<String> textRDD = spark.sparkContext().textFile(csvPath2, 1).toJavaRDD();
        JavaRDD<Row> rowJavaRDD = textRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(",");
                return RowFactory.create(attributes[0], attributes[1], attributes[2], attributes[3], attributes[4].trim());
            }
        });

        // 把之前的 schema 和 RDD 合并
        Dataset<Row> df_schema = spark.createDataFrame(rowJavaRDD, schema);
        df_schema.show();

        //创建临时表进行查询
        df_schema.createOrReplaceTempView("people");
        Dataset<Row> result = spark.sql("select id from people");
        Dataset<String> nameDS = result.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0), Encoders.STRING());
        nameDS.show();

    }

    private static void readJDBC(SparkSession spark) {
        Dataset<Row> df_jdbc = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/crashcourse")
                .option("dbtable", "dept_emp")
                .option("user", "root")
                .option("password", "123456")
//                .option("driver", "oracle.jdbc.driver.OracleDriver")
                .load();

        df_jdbc.show();
        df_jdbc.printSchema();


        StructType structType = new StructType();
        structType = structType.add("id1", DataTypes.LongType, false);
        structType = structType.add("id2", DataTypes.LongType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);


//        df_jdbc.map(new MapFunction<Row, Row>() {
//            @Override
//            public Row call(Row value) throws Exception {
//
//                new Row
//                return null;
//            }
//        }, encoder);

//        Properties properties = new Properties();
//        properties.setProperty("user", "root");
//        properties.setProperty("password", "123456");
//        df_jdbc.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/crashcourse","dept_emp",properties);
    }

    private static Dataset<Row> readFile(SparkSession spark) {
        //json
        Dataset<Row> df_json = spark.read().json("/Users/reiserx/code/bigdata/learn_geektime_bigdata/learn_spark/src/main/resources/people.json");
        df_json.show();

        //csv
        Dataset<Row> df_csv = spark.read().csv(csvPath);
        df_csv.show();

        //parquet
        Dataset<Row> df_parquet = spark.read().parquet(path_parquet);
        df_parquet.show();

        //orc
        Dataset<Row> df_orc = spark.read().orc(path_orc);
        df_orc.show();


        return df_csv;
    }

    public static class StudentGrade implements Serializable {

        private String name;
        private String subject;
        private Long grade;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public Long getGrade() {
            return grade;
        }

        public void setGrade(Long grade) {
            this.grade = grade;
        }
    }
}
