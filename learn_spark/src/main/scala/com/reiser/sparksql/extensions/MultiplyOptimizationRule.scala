package com.reiser.sparksql.extensions

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

class MultiplyOptimizationRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right) if right.isInstanceOf[Literal]
      && right.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 =>
      left
    case Multiply(left, right) if left.isInstanceOf[Literal]
      && left.asInstanceOf[Literal].value.asInstanceOf[Int] == 1 =>
      right
  }
}
