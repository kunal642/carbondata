
package org.apache.spark.sql.secondaryindex.util

/**
 *
 */
trait SecondaryIndexCreationResult[K, V] extends Serializable {
  def getKey(key: String, value: Boolean): (K, V)
}

class SecondaryIndexCreationResultImpl extends SecondaryIndexCreationResult[String, Boolean] {
  override def getKey(key: String, value: Boolean): (String, Boolean) = (key, value)
}
