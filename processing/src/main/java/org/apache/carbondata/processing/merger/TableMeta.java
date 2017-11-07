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

package org.apache.carbondata.processing.merger;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

public class TableMeta implements Serializable {

  private static final long serialVersionUID = -1749874611119829431L;

  public CarbonTableIdentifier carbonTableIdentifier;
  public String storePath;
  public CarbonTable carbonTable;
  public String tablePath;

  public TableMeta(CarbonTableIdentifier carbonTableIdentifier, String storePath, String tablePath,
      CarbonTable carbonTable) {
    this.carbonTableIdentifier = carbonTableIdentifier;
    this.storePath = storePath;
    this.tablePath = tablePath;
    this.carbonTable = carbonTable;
  }

  public CarbonTable getCarbonTable() {
    return carbonTable;
  }

}
