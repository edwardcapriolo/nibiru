package io.teknek.nibiru.engine.atom;

import org.apache.http.util.ByteArrayBuffer;

import io.teknek.nibiru.engine.SsTableReader;

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
  public byte [] externalize(){
    //TODO use byte buffer

    ByteArrayBuffer bb1 = new ByteArrayBuffer(100);
    
    bb1.append('C');
    byte [] cr = String.valueOf(getCreateTime()).getBytes();
    bb1.append(cr, 0 , cr.length);
    bb1.append(SsTableReader.END_COLUMN_PART);
    cr = String.valueOf(getTime()).getBytes();
    bb1.append(cr, 0 , cr.length);
    bb1.append(SsTableReader.END_COLUMN_PART);
    cr = String.valueOf(getTtl()).getBytes();
    bb1.append(cr,0,cr.length);
    bb1.append(SsTableReader.END_COLUMN_PART);
    cr = String.valueOf(getValue()).getBytes();
    bb1.append(SsTableReader.END_COLUMN_PART);
    bb1.append(cr,0,cr.length);
    
    return bb1.toByteArray();
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
