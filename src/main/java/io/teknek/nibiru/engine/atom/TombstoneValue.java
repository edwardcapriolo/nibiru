package io.teknek.nibiru.engine.atom;

public class TombstoneValue extends AtomValue {

  private final long time;
  
  public TombstoneValue(long time){
    this.time = time;
  }
  
  public long getTime() {
    return time;
  }

  @Override
  public byte [] externalize() {
    //todo byte buffer
    StringBuffer sb = new StringBuffer();
    sb.append('T');
    sb.append(String.valueOf(time));
    return sb.toString().getBytes();
  }
}
