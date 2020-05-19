/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, AttributeReference, EqualTo, ExprId, In, ListQuery, NamedExpression}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonPreInsertionCasts}
import org.apache.spark.sql.parser.CarbonExtensionSqlParser
import org.apache.spark.sql.secondaryindex.optimizer.CarbonSecondaryIndexOptimizer

/**
 * use SparkSessionExtensions to inject Carbon's extensions.
 * @since 2.0
 */
class CarbonExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Carbon internal parser
    extensions
      .injectParser((sparkSession: SparkSession, parser: ParserInterface) =>
        new CarbonExtensionSqlParser(new SQLConf, sparkSession, parser))

    // carbon analyzer rules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))

    // carbon optimizer rules
    extensions.injectPostHocResolutionRule((session: SparkSession) => CarbonOptimizerRule(session))

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

    // init CarbonEnv
    CarbonEnv.init()
  }
}

case class CarbonOptimizerRule(session: SparkSession) extends Rule[LogicalPlan] {
  self =>

  var notAdded = true

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (notAdded) {
      self.synchronized {
        if (notAdded) {
          notAdded = false

          val sessionState = session.sessionState
          val field = sessionState.getClass.getDeclaredField("optimizer")
          field.setAccessible(true)
          field.set(sessionState,
            new CarbonOptimizer(session, sessionState.catalog, sessionState.optimizer))
        }
      }
    }

    val x = CarbonUtils.collectCarbonRelation(plan).toList match {
      case relation :: _ =>
        val sessionParams = CarbonEnv.getInstance(session).carbonSessionInfo.getSessionParams
        val isPOCFeature = sessionParams.getProperty("carbon.ispoc", "false").toBoolean
        if (isPOCFeature) {
          plan transform {
            case a@Filter(condition, child) =>
              condition match {
                case ArrayContains(left, right) =>
                  val tableName = relation.carbonTable.getTableName + "_" +
                                  left.asInstanceOf[AttributeReference].name
                  val rewrittenPlan = new CarbonSecondaryIndexOptimizer(session).retrievePlan(
                    session.sessionState
                      .catalog.lookupRelation(TableIdentifier(tableName,
                      Some(relation.carbonTable.getDatabaseName))))(session)
                  val refCol = sessionParams.getProperty(
                    "carbon.dummysi." + relation.carbonTable.getTableName + ".primarykey")
                  val parentRefCol = child.output.find(_.name.equalsIgnoreCase(refCol)).get
                  val childReferenceColExpression = rewrittenPlan
                    .output.find(_.name.equalsIgnoreCase(refCol)).get
                  val childFilterExpression = rewrittenPlan
                    .output
                    .find(_.name.equalsIgnoreCase(left.asInstanceOf[AttributeReference].name)).get
                  Filter(In(parentRefCol,
                    Seq(ListQuery(Project(Seq(childReferenceColExpression),
                      Filter(EqualTo(childFilterExpression, right),
                        SubqueryAlias(tableName, rewrittenPlan))),
                      childOutputs = Seq(childReferenceColExpression)))),
                    child)
                case _ => a
              }
          }
        } else {
          plan
        }
      case Nil => plan
    }
    x
  }
}

