package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CountingBufferedOutputStream extends BufferedOutputStream {

  private long writtenOffset;
  public CountingBufferedOutputStream(OutputStream out) {
    super(out);
  }

  @Override
  public synchronized void write(int b) throws IOException {
    super.write(b);
    writtenOffset += 1;
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    super.write(b, off, len);
    writtenOffset += len;
  }

  @Override
  public void write(byte[] b) throws IOException {
    super.write(b);
    writtenOffset += b.length;
  }

  public long getWrittenOffset() {
    return writtenOffset;
  }
  
}
