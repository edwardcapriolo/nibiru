package io.teknek.nibiru.engine.atom;

import java.io.IOException;

import io.teknek.nibiru.io.CountingBufferedOutputStream;

public abstract class AtomValue {

  protected long time;
  
  public abstract void externalize(CountingBufferedOutputStream s) throws IOException;
  
  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

}
