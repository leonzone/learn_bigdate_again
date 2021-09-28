package com.reiser.sparksql.extensions

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object RepartitionForInsertion  extends Rule[SparkPlan]{
  override def apply(plan: SparkPlan): SparkPlan = {
    plan transformDown { case i @ InsertIntoDataSourceExec(child, _, _, partitionColumns, _)

      val newChild =

      i.withNewChildren(newChild :: Nil) }
  }
}
