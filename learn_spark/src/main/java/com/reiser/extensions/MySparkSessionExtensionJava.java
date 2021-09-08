package com.reiser.extensions;

import org.apache.spark.sql.SparkSessionExtensions;
import scala.Function1;
import scala.runtime.BoxedUnit;

/**
 * @author: reiserx
 * Date:2021/9/8
 * Des:
 */


class MySparkSessionExtensionJava implements Function1<SparkSessionExtensions, BoxedUnit> {
    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        extensions.injectOptimizerRule(MyPushDownJava::new);
        return BoxedUnit.UNIT;
    }


}
