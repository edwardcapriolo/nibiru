package io.teknek.nibiru.metadata;

public class ColumnFamilyMetadata {
  private String name;
  private long tombstoneGraceMillis;
  private int flushNumberOfRowKeys = 10000;
  private int keyCachePerSsTable = 1000;
  private int maxCompactionThreshold = 4;
  private long commitlogFlushBytes = 1000;
  
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

  public int getKeyCachePerSsTable() {
    return keyCachePerSsTable;
  }

  public void setKeyCachePerSsTable(int keyCachePerSsTable) {
    this.keyCachePerSsTable = keyCachePerSsTable;
  }

  public int getMaxCompactionThreshold() {
    return maxCompactionThreshold;
  }

  public void setMaxCompactionThreshold(int maxCompactionThreshold) {
    this.maxCompactionThreshold = maxCompactionThreshold;
  }

  public long getCommitlogFlushBytes() {
    return commitlogFlushBytes;
  }

  public void setCommitlogFlushBytes(long commitlogFlushBytes) {
    this.commitlogFlushBytes = commitlogFlushBytes;
  }

  @Override
  public String toString() {
    return "ColumnFamilyMetadata [name=" + name + ", tombstoneGraceMillis=" + tombstoneGraceMillis
            + ", flushNumberOfRowKeys=" + flushNumberOfRowKeys + ", keyCachePerSsTable="
            + keyCachePerSsTable + ", maxCompactionThreshold=" + maxCompactionThreshold
            + ", commitlogFlushBytes=" + commitlogFlushBytes + "]";
  }
  
}
