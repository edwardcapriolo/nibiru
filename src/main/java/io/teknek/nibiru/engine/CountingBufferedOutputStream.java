package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CountingBufferedOutputStream extends BufferedOutputStream {

  private long writtenOffset;
  public CountingBufferedOutputStream(OutputStream out) {
    super(out);
  }

  public synchronized void writeAndCount(int b) throws IOException {
    super.write(b);
    writtenOffset++;
  }

  public void writeAndCount(byte[] b) throws IOException {
    super.write(b);
    writtenOffset += b.length;
  }

  public long getWrittenOffset() {
    return writtenOffset;
  }
  
}
