package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

public class ExtendedBlockletWrapperContainer implements Writable {

  private ExtendedBlockletWrapper[] extendedBlockletWrappers;

  public ExtendedBlockletWrapperContainer() {
  }

  public ExtendedBlockletWrapperContainer(ExtendedBlockletWrapper[] extendedBlockletWrappers) {
    this.extendedBlockletWrappers = extendedBlockletWrappers;
  }

  public List<ExtendedBlocklet> getExtendedBlocklets() throws IOException {
    List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
    for (ExtendedBlockletWrapper extendedBlockletWrapper: extendedBlockletWrappers) {
      extendedBlocklets.addAll(extendedBlockletWrapper.getExtendedBlocklets());
    }
    return extendedBlocklets;
  }

  @Override public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(extendedBlockletWrappers.length);
    Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());
    long startTime = System.currentTimeMillis();
    for (ExtendedBlockletWrapper extendedBlockletWrapper:  extendedBlockletWrappers) {
      extendedBlockletWrapper.write(dataOutput);
    }
    LOGGER.info("Time taken to write fields : " + (System.currentTimeMillis() - startTime));
  }

  @Override public void readFields(DataInput dataInput) throws IOException {
    int wrappersSize = dataInput.readInt();
    extendedBlockletWrappers = new ExtendedBlockletWrapper[wrappersSize];
    Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < wrappersSize; i++) {
      ExtendedBlockletWrapper extendedBlockletWrapper = new ExtendedBlockletWrapper();
      extendedBlockletWrapper.readFields(dataInput);
      extendedBlockletWrappers[i] = extendedBlockletWrapper;
    }
    LOGGER.info("Time taken to read fields : " + (System.currentTimeMillis() - startTime));
  }
}
