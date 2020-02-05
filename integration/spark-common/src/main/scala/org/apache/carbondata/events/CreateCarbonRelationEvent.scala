package org.apache.carbondata.events

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 *
 * @param sparkSession
 * @param carbonTable
 */
case class CreateCarbonRelationPreEvent(sparkSession: SparkSession, carbonTable: CarbonTable)
  extends Event with CreateCarbonRelationEventInfo

/**
 *
 * @param sparkSession
 * @param carbonTable
 */
case class CreateCarbonRelationPostEvent(sparkSession: SparkSession,
    carbonTable: CarbonTable,
    needLock: Boolean)
  extends Event with CreateCarbonRelationEventInfo