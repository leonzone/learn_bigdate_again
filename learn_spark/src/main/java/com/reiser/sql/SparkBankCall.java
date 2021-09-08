package com.reiser.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: reiserx
 * Date:2021/9/1
 * Des:
 */
public class SparkBankCall {
    static String source = "/Users/reiserx/Downloads/bank-additional-full.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("SparkBankCall").master("local[*]").enableHiveSupport().getOrCreate();
        StructType schema = createSchema();

        Dataset<Row> df = spark.read().schema(schema)
                .option("sep", ";")
                .option("header", true)
                .csv(source);

//        df.show(10);

        df.groupBy("marital").count().show();
        df.groupBy("job").count().show();
        df.groupBy("edu").count().show();
        //选数值类字段作为数据子集，进行描述性统计（包括频次统计，平均值，标准差，最小值，最大值）
        Dataset<Row> dsSubset = df.select("age", "dur", "campaign", "prev", "deposit").cache();
        //通过描述性统计，可以对数据进行快速地检查。比如，频次统计可以检查数据的有效行数，年龄的平均值和范围可以判断数据样本是不是符合预期。通过均值和方差可以对数据进行更深入地分析，比如，假设数据服从正态分布，年龄的均值和标准差表明了受访者的年龄大多在30~50 之间
        dsSubset.describe().show();
        //判断变量间相关性，计算变量间的协方差和相关系数，协方差表示两变量的变化方向相同或相反。age和dur的协方差为-2.3391469421265874，表示随着受访者的年龄增加，上一次访问时长减少。
        System.out.println(dsSubset.stat().cov("age", "dur"));
        System.out.println(dsSubset.stat().corr("age", "dur"));
        //交叉表,通过交叉表可以知道在每个年龄段的婚姻状态分布
        df.stat().crosstab("age", "marital").orderBy("age_marital").show(20);
//        //所有受访人的学历背景出现频率超过0.3的学历
//        String[]  cols={"edu"};
//        Row[] collect = dsSubset.stat().freqItems(cols, 0.3).collect();
//        System.out.println(collect[0]);



    }

    private static StructType createSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("job", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("marital", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("edu", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("credit_default", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("housing", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("loan", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("contact", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("month", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("day", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("dur", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("campaign", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("pdays", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("prev", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("pout", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("emp_var_rate", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("cons_price_idx", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("cons_conf_idx", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("euribor3m", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("nr_employed", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("deposit", DataTypes.StringType, true));

        return DataTypes.createStructType(fields);
    }
}
