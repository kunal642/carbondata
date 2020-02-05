
package org.apache.spark.sql.test

import java.io.{File, FilenameFilter}
import java.util.ServiceLoader

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.util.Utils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

trait TestQueryExecutorRegister {
  def sql(sqlText: String): DataFrame

  def stop()

  def sqlContext: SQLContext
}

object TestQueryExecutor {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val projectPath = new File(this.getClass.getResource("/").getPath + "../../..")
    .getCanonicalPath.replaceAll("\\\\", "/")
  LOGGER.info(s"project path: $projectPath")
  val integrationPath = s"$projectPath/integration"
  val metaStoreDB = s"$integrationPath/spark-common/target"
  val location = s"$integrationPath/spark-common/target/dbpath"
  val masterUrl = {
    val property = System.getProperty("spark.master.url")
    if (property == null) {
      "local[2]"
    } else {
      property
    }
  }

  val hdfsUrl = {
    val property = System.getProperty("hdfs.url")
    if (property == null) {
      "local"
    } else {
      LOGGER.info("HDFS PATH given : " + property)
      property
    }
  }

  val resourcesPath = if (hdfsUrl.startsWith("hdfs://")) {
    hdfsUrl
  } else {
    s"$integrationPath/spark-common-test/src/test/resources"
  }

  val pluginResourcesPath = if (hdfsUrl.startsWith("hdfs://")) {
    hdfsUrl
  } else {
    s"$projectPath/secondary_index/src/test/resources"
  }

  val storeLocation = if (hdfsUrl.startsWith("hdfs://")) {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_HDFS)
    val carbonFile = FileFactory.
      getCarbonFile(s"$hdfsUrl/store")
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/store_" + System.nanoTime()
  } else {
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_TYPE,
      CarbonCommonConstants.CARBON_LOCK_TYPE_LOCAL)
    s"$integrationPath/spark-common/target/store"
  }
  val warehouse = if (hdfsUrl.startsWith("hdfs://")) {
    val carbonFile = FileFactory.
      getCarbonFile(s"$hdfsUrl/warehouse")
    FileFactory.deleteAllCarbonFilesOfDir(carbonFile)
    s"$hdfsUrl/warehouse_" + System.nanoTime()
  } else {
    s"$integrationPath/spark-common/target/warehouse"
  }

  val hiveresultpath = if (hdfsUrl.startsWith("hdfs://")) {
    val p = s"$hdfsUrl/hiveresultpath"
    FileFactory.mkdirs(p)
    p
  } else {
    val p = s"$integrationPath/spark-common/target/hiveresultpath"
    new File(p).mkdirs()
    p
  }

  LOGGER.info(s"""Store path taken $storeLocation""")
  LOGGER.info(s"""Warehouse path taken $warehouse""")
  LOGGER.info(s"""Resource path taken $resourcesPath""")
  LOGGER.info(s"""Plugin Resource path taken $pluginResourcesPath""")

  lazy val modules = Seq(TestQueryExecutor.projectPath + "/common/target",
    TestQueryExecutor.projectPath + "/core/target",
    TestQueryExecutor.projectPath + "/hadoop/target",
    TestQueryExecutor.projectPath + "/processing/target",
    TestQueryExecutor.projectPath + "/integration/spark-datasource/target",
    TestQueryExecutor.projectPath + "/integration/spark-common/target",
    TestQueryExecutor.projectPath + "/integration/spark2/target",
    TestQueryExecutor.projectPath + "/integration/spark-common/target/jars")
  lazy val jars = {
    val jarsLocal = new ArrayBuffer[String]()
    modules.foreach { path =>
      val files = new File(path).listFiles(new FilenameFilter {
        override def accept(dir: File, name: String) = {
          name.endsWith(".jar")
        }
      })
      files.foreach(jarsLocal += _.getAbsolutePath)
    }
    jarsLocal
  }
  CarbonProperties.getInstance()
    .addProperty("spark.sql.extensions",
      "org.apache.spark.sql.CarbonInternalExtensions")
  val INSTANCE = lookupQueryExecutor.newInstance().asInstanceOf[TestQueryExecutorRegister]
  CarbonProperties.getInstance()
    .addProperty(CarbonCommonConstants.CARBON_BAD_RECORDS_ACTION, "FORCE")
    .addProperty(CarbonCommonConstants.CARBON_BADRECORDS_LOC, "/tmp/carbon/badrecords")
    .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "1024")
    .addProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE, "1024")

  private def lookupQueryExecutor: Class[_] = {
    ServiceLoader.load(classOf[TestQueryExecutorRegister], Utils.getContextOrSparkClassLoader)
      .iterator().next().getClass
  }

}
