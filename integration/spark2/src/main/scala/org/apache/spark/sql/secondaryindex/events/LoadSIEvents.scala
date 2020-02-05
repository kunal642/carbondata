
package org.apache.spark.sql.secondaryindex.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.events.{Event, LoadEventInfo}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * Class for handling operations before start of a load process.
 * Example usage: For validation purpose
 */
case class LoadTableSIPreExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel,
    indexCarbonTable: CarbonTable) extends Event with LoadEventInfo

/**
 * Class for handling operations after data load completion and before final
 * commit of load operation. Example usage: For loading pre-aggregate tables
 */
case class LoadTableSIPostExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel,
    carbonTable: CarbonTable)
  extends Event with LoadEventInfo

/**
 * Class for handling clean up in case of any failure and abort the operation.
 */
case class LoadTableSIAbortExecutionEvent(sparkSession: SparkSession,
    carbonTableIdentifier: CarbonTableIdentifier,
    carbonLoadModel: CarbonLoadModel) extends Event with LoadEventInfo
