
package org.apache.carbondata.spark.testsuite.mergeindex

import java.io.{File, PrintWriter}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import scala.util.Random

import org.apache.carbondata.spark.core.CarbonCommonConstants

class CarbonIndexFileMergeTestCaseWithSI
  extends QueryTest with BeforeAndAfterEach with BeforeAndAfterAll {
  val file2 = resourcesPath + "/compaction/fil2.csv"

  override protected def beforeAll(): Unit = {
    val n = 150000
    createFile(file2, n * 4, n)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE, "false")
    sql("use default")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
  }

  override protected def afterAll(): Unit = {
    deleteFile(file2)
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index1 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index2 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP INDEX IF EXISTS indexmerge_index on indexmerge")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
      .addProperty(CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE,
        CarbonCommonConstants.DEFAULT_CARBON_SI_SEGMENT_MERGE)
  }

  test("Verify correctness of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index on nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql("CREATE INDEX nonindexmerge_index on table nonindexmerge (name) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index", "0") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("DROP TABLE IF EXISTS indexmerge")
    sql(
      """
        | CREATE TABLE indexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        |  TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql("CREATE INDEX indexmerge_index1 on table indexmerge (name) AS 'carbondata'")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE indexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    assert(getIndexFileCount("default_indexmerge", "0") == 0)
    assert(getIndexFileCount("default_indexmerge_index1", "0") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""),
      sql("""Select count(*) from indexmerge"""))
  }

  test("Verify command of index merge") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql("CREATE INDEX nonindexmerge_index1 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index1", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index1", "1") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index1", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index1", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify command of index merge without enabling property") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql("CREATE INDEX nonindexmerge_index2 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index2", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index2", "1") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'SEGMENT_INDEX'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index2", "0") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index2", "1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
  }

  test("Verify index index merge with compaction") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
      .addProperty(CarbonCommonConstants.ENABLE_AUTO_LOAD_MERGE, "false")
    sql("DROP INDEX IF EXISTS nonindexmerge_index3 on nonindexmerge")
    sql("DROP INDEX IF EXISTS nonindexmerge_index4 on nonindexmerge")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql("CREATE INDEX nonindexmerge_index3 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index3", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index3", "1") == 100)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "true")
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index3", "0.1") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  test("Verify index index merge for compacted segments") {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_MERGE_INDEX_IN_SEGMENT, "false")
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD, "2,2")
    sql("DROP TABLE IF EXISTS nonindexmerge")
    sql(
      """
        | CREATE TABLE nonindexmerge(id INT, name STRING, city STRING, age INT)
        | STORED AS carbondata
        | TBLPROPERTIES('SORT_COLUMNS'='city,name', 'SORT_SCOPE'='GLOBAL_SORT')
      """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql(s"LOAD DATA LOCAL INPATH '$file2' INTO TABLE nonindexmerge OPTIONS('header'='false', " +
        s"'GLOBAL_SORT_PARTITIONS'='100')")
    sql("CREATE INDEX nonindexmerge_index4 on table nonindexmerge (name) AS 'carbondata'")
    val rows = sql("""Select count(*) from nonindexmerge""").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "3") == 100)
    sql("ALTER TABLE nonindexmerge COMPACT 'minor'").collect()
    sql("ALTER TABLE nonindexmerge COMPACT 'segment_index'").collect()
    assert(getIndexFileCount("default_nonindexmerge", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "2.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge", "0.2") == 0)
    assert(getIndexFileCount("default_nonindexmerge_index4", "0") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "2") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "3") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "0.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "2.1") == 100)
    assert(getIndexFileCount("default_nonindexmerge_index4", "0.2") == 0)
    checkAnswer(sql("""Select count(*) from nonindexmerge"""), rows)
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.COMPACTION_SEGMENT_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD)
  }

  private def getIndexFileCount(tableName: String, segment: String): Int = {
    val table = CarbonMetadata.getInstance().getCarbonTable(tableName)
    val path = CarbonTablePath
      .getSegmentPath(table.getAbsoluteTableIdentifier.getTablePath, segment)
    val carbonFiles = FileFactory.getCarbonFile(path).listFiles(new CarbonFileFilter {
      override def accept(file: CarbonFile): Boolean = file.getName.endsWith(CarbonTablePath
        .INDEX_FILE_EXT)
    })
    if (carbonFiles != null) {
      carbonFiles.length
    } else {
      0
    }
  }

  private def createFile(fileName: String, line: Int = 10000, start: Int = 0): Boolean = {
    try {
      val write = new PrintWriter(fileName);
      for (i <- start until (start + line)) {
        write
          .println(i + "," + "n" + i + "," + "c" + Random.nextInt(line) + "," + Random.nextInt(80))
      }
      write.close()
    } catch {
      case _: Exception => false
    }
    true
  }

  private def deleteFile(fileName: String): Boolean = {
    try {
      val file = new File(fileName)
      if (file.exists()) {
        file.delete()
      }
    } catch {
      case _: Exception => false
    }
    true
  }

}
