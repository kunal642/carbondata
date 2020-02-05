
package org.apache.carbondata.core.metadata.schema.indextable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.util.ObjectSerializationUtil;

/**
 * Secondary Index properties holder
 */
public class IndexMetadata implements Serializable {

  private static final long serialVersionUID = -8076464279248926823L;
  /**
   * index table name and its corresponding cols
   */
  private Map<String, List<String>> indexTableMap;
  /**
   * parent table name of this index table
   */
  private String parentTableName;
  /**
   * location of parent table, path till table folder
   */
  private String parentTablePath;
  /**
   * table ID of parent table
   */
  private String parentTableId;
  /**
   * flag to check for index table
   */
  private boolean isIndexTable;

  public IndexMetadata(boolean isIndexTable) {
    this.isIndexTable = isIndexTable;
  }

  public IndexMetadata(String parentTableName, boolean isIndexTable, String parentTablePath) {
    this(isIndexTable);
    this.parentTableName = parentTableName;
    this.parentTablePath = parentTablePath;
  }

  public IndexMetadata(Map<String, List<String>> indexTableMap, String parentTableName,
      boolean isIndexTable, String parentTablePath, String parentTableId) {
    this(parentTableName, isIndexTable, parentTablePath);
    this.indexTableMap = indexTableMap;
    this.parentTableId = parentTableId;
  }

  public void addIndexTableInfo(String tableName, List<String> indexCols) {
    if (null == indexTableMap) {
      indexTableMap = new ConcurrentHashMap<String, List<String>>();
    }
    indexTableMap.put(tableName, indexCols);
  }

  public void removeIndexTableInfo(String tableName) {
    if (null != indexTableMap) {
      indexTableMap.remove(tableName);
    }
  }

  public List<String> getIndexTables() {
    if (null != indexTableMap) {
      return new ArrayList<String>(indexTableMap.keySet());
    } else {
      return new ArrayList<String>();
    }
  }

  /**
   * indexTableMap will be null if index table info is not loaded.
   */
  public Map<String, List<String>> getIndexesMap() {
    return indexTableMap;
  }

  public String getParentTableName() {
    return parentTableName;
  }

  public boolean isIndexTable() {
    return isIndexTable;
  }

  public String getParentTablePath() {
    return parentTablePath;
  }

  public String getParentTableId() {
    return parentTableId;
  }

  public String serialize() throws IOException {
    String serializedIndexMeta = ObjectSerializationUtil.convertObjectToString(this);
    return serializedIndexMeta;
  }

  public static IndexMetadata deserialize(String serializedIndexMeta) throws IOException {
    if (null == serializedIndexMeta) {
      return null;
    }
    return (IndexMetadata) ObjectSerializationUtil.convertStringToObject(serializedIndexMeta);
  }
}
