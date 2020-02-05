
package org.apache.spark.sql.secondaryindex.events

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{CreateCarbonRelationPostEvent, Event, OperationContext, OperationEventListener}


/**
 *
 */
class CreateCarbonRelationEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case createCarbonRelationPostEvent: CreateCarbonRelationPostEvent =>
        LOGGER.debug("Create carbon relation post event listener called")
        val carbonTable = createCarbonRelationPostEvent.carbonTable
        val databaseName = createCarbonRelationPostEvent.carbonTable.getDatabaseName
        val tableName = createCarbonRelationPostEvent.carbonTable.getTableName
        val sparkSession = createCarbonRelationPostEvent.sparkSession
        CarbonInternalMetastore
          .refreshIndexInfo(databaseName,
            tableName,
            carbonTable,
            createCarbonRelationPostEvent.needLock)(sparkSession)
    }
  }
}
