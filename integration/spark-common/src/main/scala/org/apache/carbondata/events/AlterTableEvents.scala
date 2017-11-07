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

package org.apache.carbondata.events

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.execution.command.{AlterTableDropColumnModel, AlterTableRenameModel}

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 *
 */
case class AlterTableDropColumnPreEvent(carbonTable: CarbonTable,
    alterTableDropColumnModel: AlterTableDropColumnModel,
    sparkSession: SparkSession) extends AlterTableDropColumnEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableDropColumnPreEvent.eventType
  }
}

case class AlterTableRenamePreEvent(carbonTable: CarbonTable,
    alterTableRenameModel: AlterTableRenameModel, newTablePath: String,
    sparkSession: SparkSession) extends AlterTableRenameEvent {

  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableRenamePreEvent.eventType
  }
}

case class AlterTableCompactionPreEvent(carbonTable: CarbonTable,
    carbonLoadModel: CarbonLoadModel,
    mergedLoadName: String,
    sQLContext: SQLContext) extends AlterTableCompactionEvent {
  /**
   * Method for getting the event type. Used for invoking all listeners registered for an event
   *
   * @return
   */
  override def getEventType: String = {
    AlterTableCompactionPreEvent.eventType
  }
}

object AlterTableDropColumnPreEvent {
  val eventType = AlterTableDropColumnPreEvent.getClass.getName
}

object AlterTableRenamePreEvent {
  val eventType = AlterTableRenamePreEvent.getClass.getName
}

object AlterTableCompactionPreEvent {
  val eventType = AlterTableCompactionPreEvent.getClass.getName
}
