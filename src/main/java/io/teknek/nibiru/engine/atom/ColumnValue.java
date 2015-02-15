package io.teknek.nibiru.engine.atom;

import java.io.IOException;

import io.teknek.nibiru.engine.SsTableReader;
import io.teknek.nibiru.io.CountingBufferedOutputStream;

public class ColumnValue extends AtomValue {

  private String value;

  private long createTime;
  private long ttl;
  
  public ColumnValue(){
    
  }
  
  public ColumnValue(String value, long time, long createTime, long ttl){
    this.value = value;
    this.time = time;
    this.createTime = createTime;
    this.ttl = ttl;
  }
  
  @Override
  public void externalize(CountingBufferedOutputStream ssOutputStream) throws IOException {
    ssOutputStream.writeAndCount("C".getBytes());
    ssOutputStream.writeAndCount(String.valueOf(getCreateTime()).getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
    ssOutputStream.writeAndCount(String.valueOf(getTime()).getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
    ssOutputStream.writeAndCount(String.valueOf(getTtl()).getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
    ssOutputStream.writeAndCount(String.valueOf(getValue()).getBytes());
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

  public long getCreateTime() {
    return createTime;
  }

  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  public long getTtl() {
    return ttl;
  }

  public void setTtl(long ttl) {
    this.ttl = ttl;
  }

  @Override
  public String toString() {
    return "ColumnValue [value=" + value + ", createTime=" + createTime + ", ttl=" + ttl + "]";
  }

  
}
