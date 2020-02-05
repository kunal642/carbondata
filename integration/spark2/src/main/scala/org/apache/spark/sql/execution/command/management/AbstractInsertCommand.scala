package org.apache.spark.sql.execution.command.management

import java.util

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.execution.command.UpdateTableModel
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.{CarbonEnv, Column, DataFrame, Row, SparkSession}

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonLoadOptionConstants, SortScopeOptions}
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.events.{BuildDataMapPostExecutionEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.model.{CarbonLoadModel, CarbonLoadModelBuilder, LoadOption}
import org.apache.carbondata.processing.util.{CarbonBadRecordUtil, CarbonLoaderUtil}
import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.optimizer.CarbonFilters

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.TableDataMap
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.mutate.{CarbonUpdateUtil, TupleIdEnum}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostExecutionEvent
import org.apache.carbondata.spark.util.CarbonScalaUtil

abstract class AbstractInsertCommand(databaseNameOp: Option[String],
    tableName: String,
    loadOptions: Map[String, String], isOverwriteTable: Boolean,
    updateModel: Option[UpdateTableModel] = None,
    tableInfoOp: Option[TableInfo] = None,
    operationContext: OperationContext,
    partition: Map[String, Option[String]] = Map.empty)(implicit sparkSession: SparkSession) {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  val hadoopConf: Configuration = sparkSession.sessionState.newHadoopConf()

  ThreadLocalSessionInfo.setConfigurationToCurrentThread(hadoopConf)

  val dbName: String = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)

  val carbonTable: CarbonTable = if (tableInfoOp.isDefined) {
    CarbonTable.buildFromTableInfo(tableInfoOp.get)
  } else {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(databaseNameOp, tableName)(sparkSession).asInstanceOf[CarbonRelation]
    if (relation == null) {
      throw new NoSuchTableException(dbName, tableName)
    }
    if (null == relation.carbonTable) {
      LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
      throw new NoSuchTableException(dbName, tableName)
    }
    relation.carbonTable
  }

  def process()

  def getFinalLoadOptions(carbonProperty: CarbonProperties,
      table: CarbonTable,
      options: Map[java.lang.String, String]): util.Map[String, String] = {
    val tableProperties = table.getTableInfo.getFactTable.getTableProperties
    val optionsFinal = LoadOption.fillOptionWithDefaultValue(options.asJava)
    //    EnvHelper.setDefaultHeader(sparkSession, optionsFinal)
    /**
     * Priority of sort_scope assignment :
     * -----------------------------------
     *
     * 1. Load Options  ->
     * LOAD DATA INPATH 'data.csv' INTO TABLE tableName OPTIONS('sort_scope'='no_sort')
     *
     * 2. Session property CARBON_TABLE_LOAD_SORT_SCOPE  ->
     * SET CARBON.TABLE.LOAD.SORT.SCOPE.database.table=local_sort
     *
     * 3. Sort Scope provided in TBLPROPERTIES
     * 4. Session property CARBON_OPTIONS_SORT_SCOPE
     * 5. Default Sort Scope LOAD_SORT_SCOPE
     */
    if (table.getNumberOfSortColumns == 0) {
      // If tableProperties.SORT_COLUMNS is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        SortScopeOptions.SortScope.NO_SORT.name)
    } else if (StringUtils.isBlank(tableProperties.get(CarbonCommonConstants.SORT_SCOPE))) {
      // If tableProperties.SORT_COLUMNS is not null
      // and tableProperties.SORT_SCOPE is null
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE +
                                     table.getDatabaseName + "." + table.getTableName,
            carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
              carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                SortScopeOptions.SortScope.LOCAL_SORT.name)))))
    } else {
      optionsFinal.put(CarbonCommonConstants.SORT_SCOPE,
        options.getOrElse(CarbonCommonConstants.SORT_SCOPE,
          carbonProperty.getProperty(
            CarbonLoadOptionConstants.CARBON_TABLE_LOAD_SORT_SCOPE + table.getDatabaseName + "." +
            table.getTableName,
            tableProperties.asScala.getOrElse(CarbonCommonConstants.SORT_SCOPE,
              carbonProperty.getProperty(CarbonLoadOptionConstants.CARBON_OPTIONS_SORT_SCOPE,
                carbonProperty.getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE,
                  CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))))))
      if (!StringUtils.isBlank(tableProperties.get("global_sort_partitions"))) {
        if (options.get("global_sort_partitions").isEmpty) {
          optionsFinal.put(
            "global_sort_partitions",
            tableProperties.get("global_sort_partitions"))
        }
      }
    }
    optionsFinal
      .put("bad_record_path", CarbonBadRecordUtil.getBadRecordsPath(options.asJava, table))
    optionsFinal
  }

  def getDataFrameWithTupleID(dataFrame: Option[DataFrame]): DataFrame = {
    val fields = dataFrame.get.schema.fields
    import org.apache.spark.sql.functions.udf
    // extracting only segment from tupleId
    val getSegIdUDF = udf((tupleId: String) =>
      CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID))
    // getting all fields except tupleId field as it is not required in the value
    val otherFields = CarbonScalaUtil.getAllFieldsWithoutTupleIdField(fields)
    // extract tupleId field which will be used as a key
    val segIdColumn = getSegIdUDF(new Column(UnresolvedAttribute
      .quotedString(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))).
      as(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_SEGMENTID)
    val fieldWithTupleId = otherFields :+ segIdColumn
    // use dataFrameWithTupleId as loadDataFrame
    val dataFrameWithTupleId = dataFrame.get.select(fieldWithTupleId: _*)
    (dataFrameWithTupleId)
  }

  def prepareLoadModel(
      factPath: String,
      parentTablePath: String,
      isDataFrame: Boolean): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setParentTablePath(parentTablePath)
    carbonLoadModel.setFactFilePath(factPath)
    carbonLoadModel.setCarbonTransactionalTable(carbonTable.getTableInfo.isTransactionalTable)
    carbonLoadModel.setAggLoadRequest(
      loadOptions.getOrElse(CarbonCommonConstants.IS_INTERNAL_LOAD_CALL,
        CarbonCommonConstants.IS_INTERNAL_LOAD_CALL_DEFAULT).toBoolean)
    carbonLoadModel.setSegmentId(loadOptions.getOrElse("mergedSegmentName", ""))
    val columnCompressor = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.COMPRESSOR,
        CompressorFactory.getInstance().getCompressor.getName)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel.setRangePartitionColumn(carbonTable.getRangeColumn)
    val javaPartition = mutable.Map[String, String]()
    partition.foreach { case (k, v) =>
      if (v.isEmpty) {
        javaPartition(k) = null
      } else {
        javaPartition(k) = v.get
      }
    }
    new CarbonLoadModelBuilder(carbonTable).build(
      loadOptions.asJava,
      carbonLoadModel,
      hadoopConf,
      javaPartition.asJava,
      isDataFrame)
    carbonLoadModel
  }

  def getCurrentParitions: util.List[PartitionSpec] = {
    if (carbonTable.isHivePartitionTable) {
      CarbonFilters.getCurrentPartitions(
        sparkSession,
        carbonTable) match {
        case Some(parts) => parts.toList.asJava
        case _ => null
      }
    } else {
      null
    }
  }

  def getCompletePartitionValues: Map[String, Option[String]] = {
    if (partition.nonEmpty) {
      val lowerCasePartitionMap = partition.map { entry =>
        (entry._1.toLowerCase, entry._2)
      }
      val partitionsColumns = carbonTable.getPartitionInfo.getColumnSchemaList
      if (partition.size != partitionsColumns.size()) {
        val dynamicPartitions = {
          partitionsColumns
            .asScala
            .filter { column =>
              !lowerCasePartitionMap.contains(column.getColumnName)
            }
            .map { column =>
              (column.getColumnName, None)
            }
        }
        partition ++ dynamicPartitions
      } else {
        partition
      }
    } else {
      partition
    }
  }

  def firePostLoadEvents(
      carbonLoadModel: CarbonLoadModel,
      tableDataMaps: util.List[TableDataMap],
      dataMapOperationContext: OperationContext,
      operationContext: OperationContext): Unit = {
    val loadTablePostExecutionEvent: LoadTablePostExecutionEvent =
      new LoadTablePostExecutionEvent(
        carbonTable.getCarbonTableIdentifier,
        carbonLoadModel)
    OperationListenerBus.getInstance.fireEvent(loadTablePostExecutionEvent, operationContext)
    if (tableDataMaps.size() > 0) {
      val buildDataMapPostExecutionEvent = BuildDataMapPostExecutionEvent(sparkSession,
        carbonTable.getAbsoluteTableIdentifier, null, Seq(carbonLoadModel.getSegmentId), false)
      OperationListenerBus.getInstance()
        .fireEvent(buildDataMapPostExecutionEvent, dataMapOperationContext)
    }
  }

  def processMetadataCommon(): (Long, LogicalRelation) = {
    if (carbonTable.isHivePartitionTable) {
      val logicalPartitionRelation =
        new FindDataSourceTable(sparkSession).apply(
          sparkSession.sessionState.catalog.lookupRelation(
            TableIdentifier(tableName, databaseNameOp))).collect {
          case l: LogicalRelation => l
        }.head
      (logicalPartitionRelation.relation.sizeInBytes, logicalPartitionRelation)
    } else {
      (0L, null)
    }
  }


}
