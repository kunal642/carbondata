
package org.apache.spark.sql.secondaryindex.load;

import java.util.Comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * This class is for comparing the two mdkeys in no kettle flow.
 */
public class RowComparatorWithOutKettle implements Comparator<Object[]> {

  /**
   * noDictionaryColMaping mapping of dictionary dimensions and no dictionary dimensions.
   */
  private boolean[] noDictionaryColMaping;

  private DataType[] noDicDataTypes;

  public RowComparatorWithOutKettle(boolean[] noDictionaryColMaping, DataType[] noDicDataTypes) {
    this.noDictionaryColMaping = noDictionaryColMaping;
    this.noDicDataTypes = noDicDataTypes;
  }

  /**
   * Below method will be used to compare two mdkeys
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;
    int index = 0;
    int noDictionaryIndex = 0;
    int dataTypeIdx = 0;
    int[] leftMdkArray = (int[]) rowA[0];
    int[] rightMdkArray = (int[]) rowB[0];
    Object[] leftNonDictArray = (Object[]) rowA[1];
    Object[] rightNonDictArray = (Object[]) rowB[1];
    for (boolean isNoDictionary : noDictionaryColMaping) {
      if (isNoDictionary) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[dataTypeIdx])) {
          // use data types based comparator for the no dictionary measure columns
          SerializableComparator comparator = org.apache.carbondata.core.util.comparator.Comparator
              .getComparator(noDicDataTypes[dataTypeIdx]);
          int difference = comparator
              .compare(leftNonDictArray[noDictionaryIndex], rightNonDictArray[noDictionaryIndex]);
          if (difference != 0) {
            return difference;
          }
        } else {
          diff = UnsafeComparer.INSTANCE.compareTo((byte[]) leftNonDictArray[noDictionaryIndex],
              (byte[]) rightNonDictArray[noDictionaryIndex]);
          if (diff != 0) {
            return diff;
          }
        }
        noDictionaryIndex++;
        dataTypeIdx++;
      } else {
        diff = leftMdkArray[index] - rightMdkArray[index];
        if (diff != 0) {
          return diff;
        }
        index++;
      }

    }
    return diff;
  }
}
