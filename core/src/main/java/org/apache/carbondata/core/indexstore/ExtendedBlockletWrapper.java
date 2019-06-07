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
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.compression.SnappyCompressor;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

/**
 * Wrapper to write and read the list of extended blocklets over RPC communication.
 */
public class ExtendedBlockletWrapper implements Writable, Serializable {

  /**
   * LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ExtendedBlockletWrapper.class.getName());
  private transient List<ExtendedBlocklet> extendedBlocklets;

  private byte[] bytes;

  private boolean isWrittenToFile;

  private int dataSize;

  private transient String queryId;

  private final int BUFFER_SIZE = 8 * 1024 * 1024;

  public ExtendedBlockletWrapper() {
    this.extendedBlocklets = new ArrayList<>();
  }

  public List<ExtendedBlocklet> getExtendedBlocklets(String filePath, String queryId)
      throws IOException {
    try {
      if (extendedBlocklets.isEmpty()) {
        readBlocklets(filePath, queryId);
      }
    } catch (IOException e) {
      LOGGER.error("Failed", e);
      throw e;
    }
    return extendedBlocklets;
  }

  public ExtendedBlockletWrapper(List<ExtendedBlocklet> extendedBlocklets, String tablePath,
      String queryId) throws IOException {
    this.extendedBlocklets = extendedBlocklets;
    this.queryId = queryId;
    long l = System.currentTimeMillis();
    byte[] data = convertToBytes();
    this.dataSize = data.length;
    LOGGER.info("*************Time taken to convert:" + (System.currentTimeMillis()-l));
    final int transferSize = 100 * 1024;
    if (this.dataSize > transferSize) {
      String outFileName = UUID.randomUUID().toString();
      this.isWrittenToFile =
          writeDataToFile(tablePath + "/" + this.queryId + "/" + outFileName, data);
      if (isWrittenToFile) {
        this.bytes = outFileName.getBytes(CarbonCommonConstants.DEFAULT_CHARSET);
      }
    } else {
      this.bytes = data;
    }
  }

  private boolean writeDataToFile(String filePath, byte[] data) {
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = FileFactory
          .getDataOutputStream(filePath, FileFactory.getFileType(filePath), BUFFER_SIZE,
              256 * 1024 * 1024, (short) 1);
      dataOutputStream.write(data);
      LOGGER.info("data is written to file");
    } catch (IOException e) {
      return false;
    } finally {
      CarbonUtil.closeStreams(dataOutputStream);
    }
    return true;
  }

  private byte[] convertToBytes() throws IOException {
    ByteArrayOutputStream bos = new ExtendedByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeInt(extendedBlocklets.size());
    for (ExtendedBlocklet extendedBlocklet : extendedBlocklets) {
      extendedBlocklet.serializeFields(dos);
    }
    byte[] bytes = new SnappyCompressor().compressByte(bos.toByteArray());
    return bytes;
  }

  private void readBlocklets(String tablePath, String queryId) throws IOException {
    byte[] data = null;
    if (isWrittenToFile) {
      DataInputStream dataInputStream = null;
      try {
        String fileName = new String(bytes, CarbonCommonConstants.DEFAULT_CHARSET);
        String filePath = tablePath + "/" + queryId + "/" + fileName;
        dataInputStream = FileFactory
            .getDataInputStream(filePath, FileFactory.getFileType(filePath), BUFFER_SIZE);
        data = new byte[dataSize];
        dataInputStream.readFully(data);
      } finally {
        CarbonUtil.closeStreams(dataInputStream);
      }
    } else {
      data = bytes;
    }
    byte[] uncompressedBytes = new SnappyCompressor().unCompressByte(data);
    ExtendedDataInputStream in = new ExtendedDataInputStream(uncompressedBytes);
    int numOfExtendedBlocklets = in.readInt();
    this.extendedBlocklets = new ArrayList<>(numOfExtendedBlocklets);
    for (int i = 0; i < numOfExtendedBlocklets; i++) {
      ExtendedBlocklet extendedBlocklet = new ExtendedBlocklet();
      extendedBlocklet.deserializeFields(in);
      extendedBlocklet.getInputSplit()
          .setTaskId(CarbonTablePath.DataFileUtil.getTaskNo(extendedBlocklet.getPath()));
      extendedBlocklet.getInputSplit()
          .setBucketId(CarbonTablePath.DataFileUtil.getBucketNo(extendedBlocklet.getPath()));
      extendedBlocklets.add(extendedBlocklet);
    }
  }

  @Override public void write(DataOutput out1) throws IOException {
    out1.writeBoolean(isWrittenToFile);
    out1.writeInt(bytes.length);
    out1.write(bytes);
    out1.writeInt(dataSize);
  }

  @Override public void readFields(DataInput in1) throws IOException {
    this.isWrittenToFile = in1.readBoolean();
    bytes = new byte[in1.readInt()];
    in1.readFully(bytes);
    this.dataSize = in1.readInt();
  }

  public static void main(String[] args) throws IOException {
    ExtendedBlockletWrapper wrapper = new ExtendedBlockletWrapper();
    wrapper.bytes = new String("f106ecfc-bad0-4536-9ae3-78e3f9470548").getBytes();
    File file = new File("C:/Users/K00900841/Documents/oldsetup/" + "jars/" + "f106ecfc-bad0-4536-9ae3-78e3f9470548");
    wrapper.dataSize = (int)file.length();
    wrapper.isWrittenToFile=true;
    wrapper.getExtendedBlocklets("C:/Users/K00900841/Documents/oldsetup/" , "jars");
  }
}
