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

package org.apache.carbondata.examples

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.{CarbonEnv, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark/src/main/resources/log4j.properties")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("error")
    Seq(
      "stored as carbondata"
    ).foreach { formatSyntax =>
      exampleBody(spark, formatSyntax)
    }
    spark.close()
  }

  def exampleBody(spark : SparkSession, formatSyntax: String = "stored as carbondata"): Unit = {

    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath

//    spark.sql("drop table if exists maintable")
//    spark.sql(
//      "create table maintable(a int, b array<string>, c array<string>) stored as carbondata " +
//      "TBLPROPERTIES('isPOC'='true', 'reference_column'='a')")
//    spark.sql(
//      "insert into maintable values (1, array('a','a','a'), array('x','x','x'))")
//    spark.sql(
//      "insert into maintable values (2, array('a','b','c'), array('x','y','z'))")
//    spark.sql("select * from maintable").show()
//    spark.sql("drop table if exists maintable_b")
//    spark.sql("drop table if exists maintable_c")
//    spark.sql("create table maintable_b(a int, b string) stored as carbondata")
//    spark.sql("insert into maintable_b values(1, 'a')")
//    spark.sql("insert into maintable_b values(2, 'a')")
//    spark.sql("insert into maintable_b values(2, 'b')")
//    spark.sql("insert into maintable_b values(2, 'c')")
//
//    spark.sql("create table maintable_c(a int, c string) stored as carbondata")
//    spark.sql("insert into maintable_c values(1, 'x')")
//    spark.sql("insert into maintable_c values(2, 'y')")
//    spark.sql("insert into maintable_c values(2, 'z')")
//    spark.sql("insert into maintable_c values(2, 'x')")
    spark.sql("select * from maintable where array_contains(b, 'a')").show()
  }
}
