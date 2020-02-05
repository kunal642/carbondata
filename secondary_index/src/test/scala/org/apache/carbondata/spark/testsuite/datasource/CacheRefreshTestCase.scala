package org.apache.carbondata.spark.testsuite.datasource

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.hive.CarbonHiveMetadataUtil
import org.scalatest.BeforeAndAfterAll

class CacheRefreshTestCase extends QueryTest with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    sql("drop database if exists cachedb cascade")
    sql("create database cachedb")
    sql("use cachedb")
  }

  override protected def afterAll(): Unit = {
    sql("use default")
    sql("drop database if exists cachedb cascade")
  }

  test("test cache refresh") {
    sql("create table tbl_cache1(col1 string, col2 int, col3 int) using carbondata")
    sql("insert into tbl_cache1 select 'a', 123, 345")
    CarbonHiveMetadataUtil.invalidateAndDropTable(
      "cachedb", "tbl_cache1", sqlContext.sparkSession)
    // discard cached table info in cachedDataSourceTables
    val tableIdentifier = TableIdentifier("tbl_cache1", Option("cachedb"))
    sqlContext.sparkSession.sessionState.catalog.refreshTable(tableIdentifier)
    sql("create table tbl_cache1(col1 string, col2 int, col3 int) using carbondata")
    sql("delete from tbl_cache1")
    sql("insert into tbl_cache1 select 'b', 123, 345")
    checkAnswer(sql("select * from tbl_cache1"),
      Seq(Row("b", 123, 345)))
  }
}
