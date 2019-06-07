package org.apache.carbondata.core.indexstore;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ExtendedDataInputStream implements DataInput {
  private DataInput dataInput;

  private ExtendedByteArrayInputStream ebis;

  public ExtendedDataInputStream(byte[] data) {
    this.ebis = new ExtendedByteArrayInputStream(data);
    this.dataInput = new DataInputStream(ebis);
  }
  @Override public void readFully(byte[] b) throws IOException {
    this.dataInput.readFully(b);
  }

  @Override public void readFully(byte[] b, int off, int len) throws IOException {
    this.dataInput.readFully(b, off, len);
  }

  @Override public int skipBytes(int n) throws IOException {
    return this.dataInput.skipBytes(n);
  }

  @Override public boolean readBoolean() throws IOException {
    return this.dataInput.readBoolean();
  }

  @Override public byte readByte() throws IOException {
    return this.dataInput.readByte();
  }

  @Override public int readUnsignedByte() throws IOException {
    return this.dataInput.readUnsignedByte();
  }

  @Override public short readShort() throws IOException {
    return this.dataInput.readShort();
  }

  @Override public int readUnsignedShort() throws IOException {
    return this.dataInput.readUnsignedShort();
  }

  @Override public char readChar() throws IOException {
    return this.dataInput.readChar();
  }

  @Override public int readInt() throws IOException {
    return this.dataInput.readInt();
  }

  @Override public long readLong() throws IOException {
    return this.dataInput.readLong();
  }

  @Override public float readFloat() throws IOException {
    return this.dataInput.readFloat();
  }

  @Override public double readDouble() throws IOException {
    return this.dataInput.readDouble();
  }

  @Override public String readLine() throws IOException {
    return this.dataInput.readLine();
  }

  @Override public String readUTF() throws IOException {
    return this.dataInput.readUTF();
  }

  public InputStream getUnderlineStream() {
    return ebis;
  }
}
