
package org.apache.spark.sql.secondaryindex.optimizer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CarbonCostBasedOptimizer {
  public static List<String> identifyRequiredTables(Set<String> filterAttributes,
      Map<String, List<String>> indexTableInfos) {
    List<String> matchedIndexTables = new ArrayList<>();

    if (filterAttributes.size() == 0) {
      return matchedIndexTables;
    }
    // This will return table name only if all the filter column matches.
    String selectedTable = identifyBestFitTable(filterAttributes, indexTableInfos);
    if (null != selectedTable) {
      matchedIndexTables.add(selectedTable);
    } else {
      Set<String> listOfTablesToBeSelected = new HashSet<>(filterAttributes.size());
      // Identify Best Fit table for each filter column
      for (String filterCol : filterAttributes) {
        Set<String> tempCol = new HashSet<>(1);
        tempCol.add(filterCol);
        String currentTable = identifyBestFitTable(tempCol, indexTableInfos);
        if (null != currentTable) {
          listOfTablesToBeSelected.add(currentTable);
        }
      }
      matchedIndexTables.addAll(listOfTablesToBeSelected);
    }
    return matchedIndexTables;
  }

  private static String identifyBestFitTable(Set<String> filterAttributes,
      Map<String, List<String>> indexTableInfos) {
    int cost = 0;
    int totalCost = 0;
    String selectedTable = null;
    Set<Map.Entry<String, List<String>>> indexTableInfosStrings = indexTableInfos.entrySet();
    for (Map.Entry<String, List<String>> indexTable : indexTableInfosStrings) {
      int currentCost = 0, currentTotalCost = 0;
      String currentTable = indexTable.getKey();
      List<String> tableCols = indexTableInfos.get(currentTable);
      if (tableCols.containsAll(filterAttributes)) {
        if (tableCols.size() == filterAttributes.size()) {
          selectedTable = currentTable;
          break;
        }
        for (int i = 0; i < tableCols.size(); i++) {
          if (filterAttributes.contains(tableCols.get(i))) {
            currentCost = currentCost + i;
          }
          currentTotalCost = currentTotalCost + i;
        }
        if (null == selectedTable) {
          selectedTable = currentTable;
          cost = currentCost;
          totalCost = currentTotalCost;
        } else if (currentCost == cost && currentTotalCost < totalCost) {
          selectedTable = currentTable;
          cost = currentCost;
          totalCost = currentTotalCost;
        } else if (currentCost < cost) {
          selectedTable = currentTable;
          cost = currentCost;
          totalCost = currentTotalCost;
        }
      }
    }
    return selectedTable;
  }
}
