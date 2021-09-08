package com.reiser.extensions;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;

/**
 * @author: reiserx
 * Date:2021/9/8
 * Des:
 */
public class MyPushDownJava extends Rule<LogicalPlan> {

    public MyPushDownJava(SparkSession spark) {
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return null;
    }
}
