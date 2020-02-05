
package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.api.CarbonStore
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.events.{DeleteSegmentByIdPostEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class DeleteSegmentByIdListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case deleteSegmentPostEvent: DeleteSegmentByIdPostEvent =>
        LOGGER.info("Delete segment By id post event listener called")
        val carbonTable = deleteSegmentPostEvent.carbonTable
        val loadIds = deleteSegmentPostEvent.loadIds
        val sparkSession = deleteSegmentPostEvent.sparkSession
        CarbonInternalScalaUtil.getIndexesTables(carbonTable).asScala.foreach { tableName =>
          val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          val table = metastore
            .lookupRelation(Some(carbonTable.getDatabaseName), tableName)(sparkSession)
            .asInstanceOf[CarbonRelation].carbonTable
          CarbonStore
            .deleteLoadById(loadIds, carbonTable.getDatabaseName, table.getTableName, table)
        }
    }
  }
}
