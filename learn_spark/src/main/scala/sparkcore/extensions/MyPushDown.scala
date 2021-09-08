package sparkcore.extensions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{EmptyRow, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    println("hello scala a new world")
   return case q: LogicalPlan => q transformExpressionsDown {
      case l: Literal => l
      case e if e.foldable => Literal.create(e.eval(EmptyRow), e.dataType)
    }
  }
}
