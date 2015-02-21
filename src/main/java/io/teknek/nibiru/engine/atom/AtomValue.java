package io.teknek.nibiru.engine.atom;

public abstract class AtomValue {

  protected long time;
  
  public abstract byte[] externalize();
  
  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

}
