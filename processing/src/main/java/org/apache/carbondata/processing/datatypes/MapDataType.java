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

package org.apache.carbondata.processing.datatypes;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.devapi.DictionaryGenerationException;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.processing.newflow.complexobjects.MapObject;
import org.apache.carbondata.processing.newflow.complexobjects.StructObject;

/**
 * Map DataType stateless object used in data loading
 */
public class MapDataType implements GenericDataType<MapObject> {

  /**
   * children columns
   */
  private List<GenericDataType> children = new ArrayList<GenericDataType>();
  /**
   * name of the column
   */
  private String name;
  /**
   * parent column name
   */
  private String parentname;
  /**
   * column unique id
   */
  private String columnId;
  /**
   * output array index
   */
  private int outputArrayIndex;
  /**
   * data counter
   */
  private int dataCounter;

  public MapDataType(String name, String parentname, String columnId) {
    this.name = name;
    this.parentname = parentname;
    this.columnId = columnId;
  }

  private MapDataType(List<GenericDataType> children, int outputArrayIndex, int dataCounter) {
    this.children = children;
    this.outputArrayIndex = outputArrayIndex;
    this.dataCounter = dataCounter;
  }

  @Override public String getName() {
    return name;
  }

  @Override public String getParentname() {
    return parentname;
  }

  @Override public void addChildren(GenericDataType children) {
    if (this.getName().equals(children.getParentname())) {
      this.children.add(children);
    } else {
      for (GenericDataType child : this.children) {
        child.addChildren(children);
      }
    }
  }

  @Override public void getAllPrimitiveChildren(List<GenericDataType> primitiveChild) {
    for (GenericDataType child : children) {
      if (child instanceof PrimitiveDataType) {
        primitiveChild.add(child);
      } else {
        child.getAllPrimitiveChildren(primitiveChild);
      }
    }
  }

  @Override public void writeByteArray(MapObject input, DataOutputStream dataOutputStream)
      throws IOException, DictionaryGenerationException {
    if (input == null) {
      for (int i = 0; i < children.size(); i++) {
        children.get(i).writeByteArray(null, dataOutputStream);
      }
    } else {
      Map<Object, Object> data = input.getData();
      dataOutputStream.writeInt(data.size());
      for (Object key: data.keySet()) {
        children.get(0).writeByteArray(key, dataOutputStream);
        children.get(1).writeByteArray(data.get(key), dataOutputStream);
      }
    }
  }

  @Override public int getSurrogateIndex() {
    return 0;
  }

  @Override public void setSurrogateIndex(int surrIndex) {

  }

  @Override
  public void parseAndBitPack(ByteBuffer byteArrayInput, DataOutputStream dataOutputStream,
      KeyGenerator[] generator) throws IOException, KeyGenException {
    int dataSize = byteArrayInput.getInt();
    dataOutputStream.writeInt(dataSize);
    if (children.get(0) instanceof PrimitiveDataType) {
      dataOutputStream.writeInt(generator[children.get(0).getSurrogateIndex()]
          .getKeySizeInBytes());
    }
    if (children.get(1) instanceof PrimitiveDataType) {
      dataOutputStream.writeInt(generator[children.get(1).getSurrogateIndex()]
          .getKeySizeInBytes());
    }
    for (int i = 0; i < dataSize; i++) {
      children.get(0).parseAndBitPack(byteArrayInput, dataOutputStream, generator);
      children.get(1).parseAndBitPack(byteArrayInput, dataOutputStream, generator);
    }
  }

  @Override public int getColsCount() {
    int colsCount = 1;
    for (GenericDataType child: children) {
      colsCount += child.getColsCount();
    }
    return colsCount;
  }

  @Override public String getColumnId() {
    return null;
  }

  @Override public void setOutputArrayIndex(int outputArrayIndex) {
    this.outputArrayIndex = outputArrayIndex;
    for (GenericDataType child: children) {
      child.setOutputArrayIndex(++outputArrayIndex);
    }
  }

  @Override public int getMaxOutputArrayIndex() {
    return 0;
  }

  @Override public void getColumnarDataForComplexType(List<ArrayList<byte[]>> columnsArray,
      ByteBuffer inputArray) {
    ByteBuffer b = ByteBuffer.allocate(8);
    int childElement = inputArray.getInt();
    b.putInt(childElement);
    if (childElement == 0) {
      b.putInt(0);
    } else {
      b.putInt(children.get(0).getDataCounter());
    }
    columnsArray.get(this.outputArrayIndex).add(b.array());
    if (children.get(0) instanceof PrimitiveDataType) {
      ((PrimitiveDataType) children.get(0)).setKeySize(inputArray.getInt());
    }
    if (children.get(1) instanceof PrimitiveDataType) {
      ((PrimitiveDataType) children.get(0)).setKeySize(inputArray.getInt());
    }
    for (int i = 0; i < childElement * 2; i++) {
      children.get(i % 2).getColumnarDataForComplexType(columnsArray, inputArray);
    }
    this.dataCounter++;
  }

  @Override public int getDataCounter() {
    return 0;
  }

  @Override
  public void fillAggKeyBlock(List<Boolean> aggKeyBlockWithComplex, boolean[] aggKeyBlock) {

  }

  @Override
  public void fillBlockKeySize(List<Integer> blockKeySizeWithComplex, int[] primitiveBlockKeySize) {
    blockKeySizeWithComplex.add(8);
    for (GenericDataType aChildren : children) {
      aChildren.fillBlockKeySize(blockKeySizeWithComplex, primitiveBlockKeySize);
    }
  }

  @Override public void fillCardinalityAfterDataLoad(List<Integer> dimCardWithComplex,
      int[] maxSurrogateKeyArray) {
    dimCardWithComplex.add(0);
    for (GenericDataType aChildren : children) {
      aChildren.fillCardinalityAfterDataLoad(dimCardWithComplex, maxSurrogateKeyArray);
    }
  }

  @Override public void fillCardinality(List<Integer> dimCardWithComplex) {
    dimCardWithComplex.add(0);
    for (GenericDataType child: children) {
      child.fillCardinality(dimCardWithComplex);
    }
  }

  @Override public GenericDataType<MapObject> deepCopy() {
    List<GenericDataType> childrenClone = new ArrayList<>();
    for (GenericDataType child : children) {
      childrenClone.add(child.deepCopy());
    }
    return new MapDataType(childrenClone, this.outputArrayIndex, this.dataCounter);
  }

}