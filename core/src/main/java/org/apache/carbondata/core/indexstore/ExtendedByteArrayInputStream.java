package org.apache.carbondata.core.indexstore;

import java.io.ByteArrayInputStream;

public class ExtendedByteArrayInputStream extends ByteArrayInputStream {
  public ExtendedByteArrayInputStream(byte[] buf) {
    super(buf);
  }

  public ExtendedByteArrayInputStream(byte[] buf, int offset, int length) {
    super(buf, offset, length);
  }
  public byte[] getBufferFromPosition() {
    byte[] data = new byte[count-pos];
    System.arraycopy(buf, pos, data, 0, data.length);
    return data;
  }

  public byte[] getBuffer() {
    return buf;
  }

  public void setPosition(int position) {
    this.pos = position;
  }

  public int getPostion() {
    return pos;
  }

}
