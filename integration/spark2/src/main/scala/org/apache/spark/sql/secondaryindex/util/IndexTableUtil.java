
package org.apache.spark.sql.secondaryindex.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.secondaryindex.exception.IndexTableExistException;
import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo;

public class IndexTableUtil {
  /**
   * adds index table info into parent table properties
   *
   */
  public static String checkAndAddIndexTable(String gsonData, IndexTableInfo newIndexTable)
      throws IndexTableExistException {
    IndexTableInfo[] indexTableInfos = IndexTableInfo.fromGson(gsonData);
    if (null == indexTableInfos) {
      indexTableInfos = new IndexTableInfo[0];
    }
    List<IndexTableInfo> indexTables =
        new ArrayList<>(Arrays.asList(indexTableInfos));
    for (IndexTableInfo indexTable : indexTableInfos) {
      if (indexTable.getIndexCols().size() == newIndexTable.getIndexCols().size()) {
        //If column order is not same, then also index table creation should be successful
        //eg., index1 is a,b,d and index2 is a,d,b. Both table creation should be successful
        boolean isColumnOrderSame = true;
        for (int i = 0; i < indexTable.getIndexCols().size(); i++) {
          if (!indexTable.getIndexCols().get(i)
              .equalsIgnoreCase(newIndexTable.getIndexCols().get(i))) {
            isColumnOrderSame = false;
            break;
          }
        }
        if (isColumnOrderSame) {
          throw new IndexTableExistException("Index Table with selected columns already exist");
        }
      }
    }
    indexTables.add(newIndexTable);
    return IndexTableInfo.toGson(indexTables.toArray(new IndexTableInfo[0]));
  }
}
