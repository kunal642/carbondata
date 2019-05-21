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

package org.apache.carbondata.indexserver

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}

import org.apache.hadoop.mapred.{RecordReader, TaskAttemptID}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{InputSplit, Job, TaskType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.{Partition, SparkEnv, TaskContext}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.{ExtendedBlocklet, ExtendedBlockletWrapper}
import org.apache.carbondata.spark.rdd.CarbonRDD
import org.apache.carbondata.spark.util.CarbonScalaUtil

private[indexserver] class DataMapRDDPartition(rddId: Int,
    idx: Int,
    val inputSplit: Seq[InputSplit],
    location: Array[String])
  extends Partition {

  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  def getLocations: Array[String] = {
    location
  }
}

private[indexserver] class DistributedPruneRDD(@transient private val ss: SparkSession,
    dataMapFormat: DistributableDataMapFormat)
  extends CarbonRDD[(String, ExtendedBlockletWrapper)](ss, Nil) {

  val executorsList: Set[String] = DistributionUtil.getExecutors(ss.sparkContext).toSet

  @transient private val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD]
    .getName)

  var readers: scala.collection.Iterator[RecordReader[Void, ExtendedBlocklet]] = _

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataMapRDDPartition].getLocations.toSeq
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, ExtendedBlockletWrapper)] = {
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(FileFactory.getConfiguration, attemptId)
    val inputSplits = split.asInstanceOf[DataMapRDDPartition].inputSplit

    val startTime = System.currentTimeMillis()
    implicit val ec: ExecutionContextExecutor = ExecutionContext
      .fromExecutor(Executors.newFixedThreadPool(2))

    val splits = Seq(inputSplits.splitAt(inputSplits.length / 2))
    val futures = splits.flatMap { split =>
      Seq(Future {
        split._2.flatMap { inputSplit =>
          val blocklets = new java.util.ArrayList[ExtendedBlocklet]()
          val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
          reader.initialize(inputSplit, attemptContext)

          while (reader.nextKeyValue()) {
            blocklets.add(reader.getCurrentValue)
          }
          reader.close()
          blocklets.asScala
        }
      },
        Future {
          split._1.flatMap { inputSplit =>
            val blocklets = new java.util.ArrayList[ExtendedBlocklet]()
            val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
            reader.initialize(inputSplit, attemptContext)

            while (reader.nextKeyValue()) {
              blocklets.add(reader.getCurrentValue)
            }
            reader.close()
            blocklets.asScala
          }
        })
    }
    val f = Await.result(Future.sequence(futures), Duration.Inf).flatten

    val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD].getName)
    LOGGER.info(s"Time taken to collect ${inputSplits.size} blocklets : " + (System.currentTimeMillis() - startTime))
    val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
      CacheProvider.getInstance().getCarbonCache.getCurrentSize
    } else {
      0L
    }
    val executorIP = SparkEnv.get.blockManager.blockManagerId.host
    val value = (executorIP + "_" + cacheSize.toString, new ExtendedBlockletWrapper(f.asJava,
      dataMapFormat.getCarbonTable.getTablePath))
    Iterator(value)
  }

  override protected def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val splits = dataMapFormat.getSplits(job).asScala
    executorsList.foreach(a => "-----------" + LOGGER.info(a))
    if (dataMapFormat.isFallbackJob || splits.isEmpty) {
      splits.zipWithIndex.map {
        f => new DataMapRDDPartition(id, f._2, List(f._1), f._1.getLocations)
      }.toArray
    } else {
      val (response, time) = CarbonScalaUtil.logTime {
        DistributedRDDUtils.getExecutors(splits.toArray, executorsList, dataMapFormat
          .getCarbonTable.getTableUniqueName, id)
      }

      LOGGER.info(s"Time taken to assign executors to ${ splits.length } is $time ms")
      response
    }
  }
}
