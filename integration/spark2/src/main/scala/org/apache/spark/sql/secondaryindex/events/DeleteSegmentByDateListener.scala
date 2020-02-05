
package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DeleteSegmentByDatePostEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class DeleteSegmentByDateListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {

    event match {
      case deleteSegmentPostEvent: DeleteSegmentByDatePostEvent =>
        LOGGER.info("Delete segment By date post event listener called")
        val carbonTable = deleteSegmentPostEvent.carbonTable
        val loadDates = deleteSegmentPostEvent.loadDates
        val sparkSession = deleteSegmentPostEvent.sparkSession
        CarbonInternalScalaUtil.getIndexesTables(carbonTable).asScala.foreach { tableName =>
          val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          val table = metastore
            .lookupRelation(Some(carbonTable.getDatabaseName), tableName)(sparkSession)
            .asInstanceOf[CarbonRelation].carbonTable
          CarbonStore
            .deleteLoadByDate(loadDates, carbonTable.getDatabaseName, table.getTableName, table)
        }
    }
  }
}
