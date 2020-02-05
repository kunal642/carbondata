

package org.apache.spark.sql.secondaryindex.events

import scala.collection.JavaConverters._

import org.apache.log4j.Logger

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, ShowTableCacheEvent}

object ShowCacheSIEventListener extends OperationEventListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>
        val carbonTable = showTableCacheEvent.carbonTable
        val internalCall = showTableCacheEvent.internalCall
        if (carbonTable.isIndexTable && !internalCall) {
          throw new UnsupportedOperationException("Operation not allowed on index table.")
        }

        val childTables = operationContext.getProperty(carbonTable.getTableUniqueName)
          .asInstanceOf[List[(String, String)]]

        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          operationContext.setProperty(carbonTable.getTableUniqueName, indexTables.map {
            indexTable => (carbonTable.getDatabaseName + "-" +indexTable, "Secondary Index")
          }.toList ++ childTables)
        }
    }
  }
}
