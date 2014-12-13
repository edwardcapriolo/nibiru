package io.teknek.nibiru.metadata;

public class ColumnFamilyMetadata {
  private String name;
  private long tombstoneGraceMillis;
  private int flushNumberOfRowKeys = 10000;
  
  public ColumnFamilyMetadata(){
    
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getTombstoneGraceMillis() {
    return tombstoneGraceMillis;
  }

  public void setTombstoneGraceMillis(long tombstoneGraceTime) {
    this.tombstoneGraceMillis = tombstoneGraceTime;
  }

  public int getFlushNumberOfRowKeys() {
    return flushNumberOfRowKeys;
  }

  public void setFlushNumberOfRowKeys(int flushNumberOfRowKeys) {
    this.flushNumberOfRowKeys = flushNumberOfRowKeys;
  }
  
}
