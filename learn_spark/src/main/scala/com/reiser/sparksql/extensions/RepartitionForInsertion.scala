package com.reiser.sparksql.extensions

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.InsertIntoDataSourceCommand

object RepartitionForInsertion  extends Rule[SparkPlan]{
  override def apply(plan: SparkPlan): SparkPlan = plan transformDown {
//    case i @ InsertIntoDataSourceCommand(logicalRelation, query, overwrite)
//      val newChild =
//      i.withNewChildren(newChild :: Nil)

  }
}
