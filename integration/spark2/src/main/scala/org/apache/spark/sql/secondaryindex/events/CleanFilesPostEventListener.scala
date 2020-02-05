
package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.events.{CleanFilesPostEvent, Event, OperationContext, OperationEventListener}

/**
 *
 */
class CleanFilesPostEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case cleanFilesPostEvent: CleanFilesPostEvent =>
        LOGGER.info("Clean files post event listener called")
        val carbonTable = cleanFilesPostEvent.carbonTable
        val indexTables = CarbonInternalScalaUtil
          .getIndexCarbonTables(carbonTable, cleanFilesPostEvent.sparkSession)
        indexTables.foreach { indexTable =>
          val partitions: Option[Seq[PartitionSpec]] = CarbonFilters.getPartitions(
            Seq.empty[Expression],
            cleanFilesPostEvent.sparkSession,
            indexTable)
          SegmentStatusManager.deleteLoadsAndUpdateMetadata(
            indexTable, true, partitions.map(_.asJava).orNull)
          CarbonUpdateUtil.cleanUpDeltaFiles(indexTable, true)
        }
    }
  }
}
