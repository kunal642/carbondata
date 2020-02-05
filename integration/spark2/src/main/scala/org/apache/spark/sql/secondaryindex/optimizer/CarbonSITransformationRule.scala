
package org.apache.spark.sql.secondaryindex.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.util.SparkUtil

/**
 * Rule for rewriting plan if query has a filter on index table column
 */
class CarbonSITransformationRule(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with PredicateHelper {

  val secondaryIndexOptimizer: CarbonSecondaryIndexOptimizer =
    new CarbonSecondaryIndexOptimizer(sparkSession)

  //TODO: Fix this
  def apply(plan: LogicalPlan): LogicalPlan = {
//    val carbonLateDecodeRule = new CarbonLateDecodeRule
//    if (carbonLateDecodeRule.checkIfRuleNeedToBeApplied(plan)) {
//      secondaryIndexOptimizer.transformFilterToJoin(plan, isProjectionNeeded(plan))
//    } else {
      plan
//    }
  }

  /**
   * Method to check whether the plan is for create/insert non carbon table(hive, parquet etc).
   * In this case, transformed plan need to add the extra projection, as positionId and
   * positionReference columns will also be added to the output of the plan irrespective of
   * whether the query has requested these columns or not
   *
   * @param plan
   * @return
   */
  private def isProjectionNeeded(plan: LogicalPlan): Boolean = {
    var needProjection = false
    if (SparkUtil.isSparkVersionXandAbove("2.3")) {
      plan collect {
        case create: CreateHiveTableAsSelectCommand =>
          needProjection = true
        case create: LogicalPlan if (create.getClass.getSimpleName
          .equals("OptimizedCreateHiveTableAsSelectCommand")) =>
          needProjection = true
        case insert: InsertIntoHadoopFsRelationCommand =>
          if (!insert.fileFormat.toString.equals("carbon")) {
            needProjection = true
          }
      }
    }
    needProjection
  }
}
