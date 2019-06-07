package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

  public List<ExtendedBlocklet> getExtendedBlocklets(String filePath, String queryId) throws IOException {
    int numberOfThreads = 4 * 2;
    ExecutorService service = Executors.newFixedThreadPool(numberOfThreads);
    int length = extendedBlockletWrappers.length;
    int numberOfBlockPerThread = length / numberOfThreads;
    int leftOver = length % numberOfThreads;
    int [] details;
    if(numberOfBlockPerThread > 0) {
      details = new int[numberOfThreads];
    } else {
      details = new int[1];
    }
    Arrays.fill(details, numberOfBlockPerThread);
    for (int i = 0; i <leftOver ; i++) {
      details[i] += 1;
    }
    int start = 0;
    int end = 0;
    List<Future<List<ExtendedBlocklet>>> future = new ArrayList<>();
    for (int i = 0; i < details.length; i++) {
      end += details[i];
      BlockDeserializer blockDeserializer =
          new BlockDeserializer(extendedBlockletWrappers, start, end, filePath, queryId);
      future.add(service.submit(blockDeserializer));
      start += details[i];
    }
    service.shutdown();
    try {
      service.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<ExtendedBlocklet> extendedBlocklets = new ArrayList<>();
    for (int i = 0; i < future.size(); i++) {
      try {
        extendedBlocklets.addAll(future.get(i).get());
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
    }

//    for (ExtendedBlockletWrapper extendedBlockletWrapper: extendedBlockletWrappers) {
//      extendedBlocklets.addAll(extendedBlockletWrapper.getExtendedBlocklets(filePath, queryId));
//    }
    return extendedBlocklets;
  }

  private class BlockDeserializer implements Callable<List<ExtendedBlocklet>> {
    private ExtendedBlockletWrapper[] extendedBlockletWrappers;
    private int start;
    private int end;
    private String filePath;
    private String queryId;

    public BlockDeserializer(ExtendedBlockletWrapper[] extendedBlockletWrappers, int start,
        int end, String filePath, String queryId) {
      this.extendedBlockletWrappers = extendedBlockletWrappers;
      this.start = start;
      this.end = end;
      this.filePath = filePath;
      this.queryId = queryId;
    }
    @Override public List<ExtendedBlocklet> call() throws Exception {
      List<ExtendedBlocklet> list = new ArrayList<>();
      for (int i = start; i < end; i++) {
        list.addAll(extendedBlockletWrappers[i].getExtendedBlocklets(filePath, queryId));
      }
      return list;
    }
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
