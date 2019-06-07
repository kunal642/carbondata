package org.apache.carbondata.core.indexstore;

import java.io.ByteArrayOutputStream;

public class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
  public int getSize() {
    return count;
  }

  public byte[] getBuffer() {
    return buf;
  }
}
