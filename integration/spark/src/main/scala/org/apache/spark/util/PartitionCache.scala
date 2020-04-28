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

package org.apache.spark.util

import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTablePartition}
import scala.collection.JavaConverters._

import org.apache.log4j.Logger

import org.apache.carbondata.common.logging.LogServiceFactory

object PartitionCache {

  //TODO: Better to use time based cache to avoid driver memory getting exhausted.
  private val CACHE = new ConcurrentHashMap[String, CacheablePartitionSpec]

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def getPartitions(identifier: AbsoluteTableIdentifier): Seq[CatalogTablePartition] = {
    val cacheablePartitionSpec = CACHE.get(identifier
      .getCarbonTableIdentifier
      .getTableId)
    if (cacheablePartitionSpec != null) {
      val tableStatusModifiedTime = FileFactory
        .getCarbonFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath))
        .getLastModifiedTime
      if (tableStatusModifiedTime > cacheablePartitionSpec.timestamp) {
        readPartitions(identifier)
      } else {
        cacheablePartitionSpec.partitionSpecs
      }
    }
    else {
      readPartitions(identifier)
    }
  }

  private def readPartitions(identifier: AbsoluteTableIdentifier) = {
    LOGGER.info("Reading partition values from store")
    val loadMetadataDetails = SegmentStatusManager.readTableStatusFile(
      CarbonTablePath.getTableStatusFilePath(identifier.getTablePath))
    val partitionSpecs = loadMetadataDetails.flatMap {
      loadDetail =>
        val segmentFileName = loadDetail.getSegmentFile
        val segmentFile = SegmentFileStore.readSegmentFile(
          CarbonTablePath.getSegmentFilePath(identifier.getTablePath, segmentFileName))
        segmentFile.getLocationMap().values().asScala.flatMap(_.getPartitions.asScala)
    }.toSet.map { uniquePartition: String =>
        val partitionSplit = uniquePartition.split("=")
      val storageFormat = CatalogStorageFormat(Some(new URI(identifier.getTablePath + "/" + uniquePartition)), None, None, None, false, Map())
        CatalogTablePartition(Map(partitionSplit(0) -> partitionSplit(1)), storageFormat)
    }.toSeq
    val cacheObject = CacheablePartitionSpec(partitionSpecs,
      FileFactory
        .getCarbonFile(CarbonTablePath.getTableStatusFilePath(identifier.getTablePath))
        .getLastModifiedTime)
    CACHE.put(identifier.getCarbonTableIdentifier.getTableId, cacheObject)
    partitionSpecs
  }
}

private case class CacheablePartitionSpec(val partitionSpecs: Seq[CatalogTablePartition],
    var timestamp: Long)