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
package org.apache.carbondata.core.indexstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.compression.SnappyCompressor;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Wrapper to write and read the list of extended blocklets over RPC communication.
 */
public class ExtendedBlockletWrapper implements Writable, Serializable {

  private transient List<ExtendedBlocklet> extendedBlocklets;

  private transient String tablePath;

  private byte[] bytes;

  public ExtendedBlockletWrapper() {
    this.extendedBlocklets = new ArrayList<>();
  }

  public ExtendedBlockletWrapper(ExtendedBlocklet[] extendedBlocklets, String tablePath) throws IOException {
    this.extendedBlocklets = Arrays.asList(extendedBlocklets);
    this.tablePath = tablePath;
  }

  public List<ExtendedBlocklet> getExtendedBlocklets() throws IOException {
    initializeBlocklets();
    return extendedBlocklets;
  }

  public ExtendedBlockletWrapper(List<ExtendedBlocklet> extendedBlocklets, String tablePath) throws IOException {
    this.extendedBlocklets = extendedBlocklets;
    this.tablePath = tablePath;
    convertToBytes();
  }

  private void convertToBytes() throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
    out.writeUTF(tablePath);
    out.writeInt(extendedBlocklets.size());
    for (ExtendedBlocklet extendedBlocklet : extendedBlocklets) {
      String path = extendedBlocklet.getPath().replace(tablePath, "");
      extendedBlocklet.setFilePath(path);
      extendedBlocklet.writeLess(out);
    }
    bytes =
        new SnappyCompressor().compressByte(byteArrayOutputStream.toByteArray());
  }

  private void initializeBlocklets() throws IOException {
    byte[] uncompressedBytes = new SnappyCompressor().unCompressByte(bytes);
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(uncompressedBytes);
    DataInputStream in = new DataInputStream(byteArrayInputStream);
    tablePath = in.readUTF();
    int numOfExtendedBlocklets = in.readInt();
    this.extendedBlocklets = new ArrayList<>(numOfExtendedBlocklets);
    for (int i = 0; i < numOfExtendedBlocklets; i++) {
      ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet();
      extendedBlocklet.readLess(in);
      extendedBlocklet.setFilePath(tablePath + extendedBlocklet.getPath());
      extendedBlocklet.getInputSplit().setFilePath(extendedBlocklet.getPath());
      extendedBlocklet.getInputSplit()
          .setTaskId(CarbonTablePath.DataFileUtil.getTaskNo(extendedBlocklet.getPath()));
      extendedBlocklet.getInputSplit()
          .setBucketId(CarbonTablePath.DataFileUtil.getBucketNo(extendedBlocklet.getPath()));
      extendedBlocklets.add(extendedBlocklet);
    }
  }

  @Override public void write(DataOutput out1) throws IOException {
    out1.writeInt(bytes.length);
    out1.write(bytes);
  }

  @Override public void readFields(DataInput in1) throws IOException {
    bytes = new byte[in1.readInt()];
    in1.readFully(bytes);
    initializeBlocklets();
  }
}
