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

package org.apache.spark.sql.execution.command.management

import java.text.SimpleDateFormat
import java.util

import org.apache.log4j.Logger
import org.apache.spark.sql.{AnalysisException, CarbonUtils, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.execution.command.{DataLoadTableFileMapping, UpdateTableModel}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.util.{CarbonReflectionUtils, CausedBy, SparkUtil}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.events.exception.PreEventException
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.exception.NoRetryException
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory

/*
* insert into with df, doesn't use logical plan
*
*/
case class CarbonInsertIntoWithDf(databaseNameOp: Option[String],
    tableName: String,
    options: Map[String, String],
    isOverwriteTable: Boolean,
    dimFilesPath: Seq[DataLoadTableFileMapping] = Seq(),
    var dataFrame: DataFrame,
    inputSqlString: String = null,
    updateModel: Option[UpdateTableModel] = None,
    tableInfoOp: Option[TableInfo] = None,
    internalOptions: Map[String, String] = Map.empty,
    partition: Map[String, Option[String]] = Map.empty,
    operationContext: OperationContext = new OperationContext)(sparkSession: SparkSession)
  extends AbstractInsertCommand() {

  def process(): Seq[Row] = {
    val (sizeInBytes, logicalPartitionRelation) = processMetadataCommon()
    val finalPartition = getCompletePartitionValues
    val carbonProperty: CarbonProperties = CarbonProperties.getInstance()
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    carbonProperty.addProperty("zookeeper.enable.lock", "false")
    val factPath = ""
    val currPartitions = getCurrentParitions
    CommonLoadUtils.setNumberOfCoresWhileLoading(sparkSession)
    val optionsFinal: util.Map[String, String] =
      getFinalLoadOptions(carbonProperty, carbonTable, options)
    val carbonLoadModel: CarbonLoadModel = prepareLoadModel(
      hadoopConf,
      factPath,
      optionsFinal,
      carbonTable,
      isDataFrame = true,
      internalOptions,
      finalPartition,
      options)
    // Delete stale segment folders that are not in table status but are physically present in
    // the Fact folder
    LOGGER.info(s"Deleting stale folders if present for table $dbName.$tableName")
    TableProcessingOperations.deletePartialLoadDataIfExist(table, false)
    var isUpdateTableStatusRequired = false
    val uuid = ""
    try {
      val (tableDataMaps, dataMapOperationContext) =
        CommonLoadUtils.firePreLoadEvents(sparkSession,
          carbonLoadModel,
          uuid,
          table,
          isOverwriteTable,
          operationContext)
      // Clean up the old invalid segment data before creating a new entry for new load.
      SegmentStatusManager.deleteLoadsAndUpdateMetadata(table, false, currPartitions)
      // add the start entry for the new load in the table status file
      if (updateModel.isEmpty && !table.isHivePartitionTable) {
        CarbonLoaderUtil.readAndUpdateLoadProgressInTableMeta(
          carbonLoadModel,
          isOverwriteTable)
        isUpdateTableStatusRequired = true
      }
      if (isOverwriteTable) {
        LOGGER.info(s"Overwrite of carbon table with $dbName.$tableName is in progress")
      }
      // Create table and metadata folders if not exist
      if (carbonLoadModel.isCarbonTransactionalTable) {
        val metadataDirectoryPath = CarbonTablePath.getMetadataPath(table.getTablePath)
        if (!FileFactory.isFileExist(metadataDirectoryPath)) {
          FileFactory.mkdirs(metadataDirectoryPath)
        }
      } else {
        carbonLoadModel.setSegmentId(System.nanoTime().toString)
      }
      val partitionStatus = SegmentStatus.SUCCESS

      val loadParams = CarbonLoadParams(sparkSession,
        sizeInBytes,
        isOverwriteTable,
        carbonLoadModel,
        hadoopConf,
        logicalPartitionRelation,
        options,
        finalPartition,
        currPartitions,
        partitionStatus,
        Some(dataFrame),
        None,
        updateModel,
        operationContext)

      LOGGER.info("Sort Scope : " + carbonLoadModel.getSortScope)
      val (rows, loadResult) = insertData(loadParams)
      val info = CommonLoadUtils.makeAuditInfo(loadResult)
      CommonLoadUtils.firePostLoadEvents(sparkSession,
        carbonLoadModel,
        tableDataMaps,
        dataMapOperationContext,
        table,
        operationContext)
    } catch {
      case CausedBy(ex: NoRetryException) =>
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        LOGGER.error(s"Dataload failure for $dbName.$tableName", ex)
        throw new RuntimeException(s"Dataload failure for $dbName.$tableName, ${ex.getMessage}")
      // In case of event related exception
      case preEventEx: PreEventException =>
        LOGGER.error(s"Dataload failure for $dbName.$tableName", preEventEx)
        throw new AnalysisException(preEventEx.getMessage)
      case ex: Exception =>
        LOGGER.error(ex)
        // update the load entry in table status file for changing the status to marked for delete
        if (isUpdateTableStatusRequired) {
          CarbonLoaderUtil.updateTableStatusForFailure(carbonLoadModel, uuid)
        }
        throw ex
    }
    Seq.empty
  }

  def insertData(loadParams: CarbonLoadParams): (Seq[Row], LoadMetadataDetails) = {
    var rows = Seq.empty[Row]
    val loadDataFrame = if (updateModel.isDefined) {
      Some(CommonLoadUtils.getDataFrameWithTupleID(Some(dataFrame)))
    } else {
      Some(dataFrame)
    }
    val table = loadParams.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadResult : LoadMetadataDetails = null
    loadParams.dataFrame = loadDataFrame
    if (table.isHivePartitionTable) {
      rows = CommonLoadUtils.loadDataWithPartition(loadParams)
    } else {
      loadResult = CarbonDataRDDFactory.loadCarbonData(loadParams.sparkSession.sqlContext,
        loadParams.carbonLoadModel,
        loadParams.partitionStatus,
        isOverwriteTable,
        loadParams.hadoopConf,
        loadDataFrame,
        None,
        updateModel,
        operationContext)
    }
    (rows, loadResult)
  }

}
