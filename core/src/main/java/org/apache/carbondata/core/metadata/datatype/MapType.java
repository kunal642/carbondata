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

package org.apache.carbondata.core.metadata.datatype;

import java.util.Objects;

public class MapType extends DataType {

  private DataType keyType;

  private DataType valueType;

  MapType(DataType keyType, DataType valueType) {
    super(DataTypes.MAP_TYPE_ID, 11, "MAP", -1);
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public boolean isComplexType() {
    return true;
  }

  public DataType getKeyType() {
    return keyType;
  }

  public DataType getValueType() {
    return valueType;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    MapType mapType = (MapType) o;
    return Objects.equals(keyType, mapType.keyType) && Objects.equals(valueType, mapType.valueType);
  }

  @Override public int hashCode() {

    return Objects.hash(super.hashCode(), keyType, valueType);
  }
}
