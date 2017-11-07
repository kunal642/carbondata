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

package org.apache.spark.sql.execution.command.preaaggregate

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.events.{DropTablePreEvent, Event, EventListener}

class DropTablePreListener extends EventListener {

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event): Unit = {
    val dropPreEvent = event.asInstanceOf[DropTablePreEvent]
    val carbonTable = dropPreEvent.carbonTable
    if (carbonTable.getTableInfo.getParentRelationIdentifiers != null &&
        !carbonTable.getTableInfo.getParentRelationIdentifiers.isEmpty) {
      val sparkSession = dropPreEvent.sparkSession
      val absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier
      val tableIdentifier = new TableIdentifier(absoluteTableIdentifier.getCarbonTableIdentifier
        .getTableName,
        Some(absoluteTableIdentifier.getCarbonTableIdentifier.getDatabaseName))
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .dropTable(absoluteTableIdentifier.getTablePath, tableIdentifier)(
          sparkSession)
    }
  }
}
