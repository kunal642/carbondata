
package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, SecondaryIndexUtil}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DeleteFromTablePostEvent, DeleteFromTablePreEvent, Event, OperationContext, OperationEventListener}

/**
 * Listener for handling delete command events
 */
class DeleteFromTableEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case deleteFromTablePreEvent: DeleteFromTablePreEvent =>
        LOGGER.info("Delete from table pre event listener called")
        val carbonTable = deleteFromTablePreEvent.carbonTable
        // Should not allow delete on index table
        if (carbonTable.isIndexTable) {
          sys
            .error(s"Delete is not permitted on Index Table [${
              carbonTable
                .getDatabaseName
            }.${ carbonTable.getTableName }]")
        }
      case deleteFromTablePostEvent: DeleteFromTablePostEvent =>
        LOGGER.info("Delete from table post event listener called")
        val parentCarbonTable = deleteFromTablePostEvent.carbonTable
        val sparkSession = deleteFromTablePostEvent.sparkSession
        CarbonInternalMetastore
          .refreshIndexInfo(parentCarbonTable.getDatabaseName,
            parentCarbonTable.getTableName,
            parentCarbonTable)(
            sparkSession)
        val indexTableList = CarbonInternalScalaUtil.getIndexesTables(parentCarbonTable)
        if (!indexTableList.isEmpty) {
          val indexCarbonTableList = indexTableList.asScala.map { indexTableName =>
            CarbonEnv.getInstance(sparkSession).carbonMetaStore
              .lookupRelation(Option(parentCarbonTable.getDatabaseName), indexTableName)(
                sparkSession)
              .asInstanceOf[CarbonRelation].carbonTable
          }.toList
          SecondaryIndexUtil
            .updateTableStatusForIndexTables(parentCarbonTable, indexCarbonTableList.asJava)
        }
    }
  }
}
