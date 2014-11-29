package io.teknek.nibiru;

public class TimeSourceImpl implements TimeSource{

  @Override
  public long getTimeInMillis() {
    return System.currentTimeMillis();
  }

}
