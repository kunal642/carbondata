
package org.apache.spark.sql.secondaryindex.events

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, UpdateTablePreEvent}

/**
 *
 */
class UpdateTablePreEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case updateTablePreEvent: UpdateTablePreEvent =>
        LOGGER.info("Update table pre event listener called")
        val carbonTable = updateTablePreEvent.carbonTable
        // Should not allow update on index table
        if (carbonTable.isIndexTable) {
          sys
            .error(s"Update is not permitted on Index Table [${
              carbonTable
                .getDatabaseName
            }.${ carbonTable.getTableName }]")
        } else if (!CarbonInternalScalaUtil.getIndexesMap(carbonTable).isEmpty) {
          sys
            .error(s"Update is not permitted on table that contains secondary index [${
              carbonTable
                .getDatabaseName
            }.${ carbonTable.getTableName }]. Drop all indexes and retry")

        }
    }
  }
}
