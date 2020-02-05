
package org.apache.spark.sql.secondaryindex.events

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{Event, LookupRelationPostEvent, OperationContext, OperationEventListener}

/**
 *
 */
class SIRefreshEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case lookupRelationPostEvent: LookupRelationPostEvent =>
        LOGGER.debug("SI Refresh post event listener called")
        val carbonTable = lookupRelationPostEvent.carbonTable
        val databaseName = lookupRelationPostEvent.carbonTable.getDatabaseName
        val tableName = lookupRelationPostEvent.carbonTable.getTableName
        val sparkSession = lookupRelationPostEvent.sparkSession
        CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    }
  }
}
