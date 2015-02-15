package io.teknek.nibiru.engine.atom;

import java.io.IOException;

import io.teknek.nibiru.io.CountingBufferedOutputStream;

public class TombstoneValue extends AtomValue {

  private final long time;
  
  public TombstoneValue(long time){
    this.time = time;
  }
  
  public long getTime() {
    return time;
  }

  @Override
  public void externalize(CountingBufferedOutputStream s) throws IOException {
    s.writeAndCount("T".getBytes());
    s.writeAndCount(String.valueOf(time).getBytes());
  }
}
