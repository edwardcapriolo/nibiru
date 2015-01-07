package io.teknek.nibiru.metadata;

import java.util.Map;

public class ColumnFamilyMetaData {
  private String name;
  private String implementingClass;
  
  private Map<String,Object> properties;
  private long tombstoneGraceMillis;
  private int flushNumberOfRowKeys = 10000;
  private int keyCachePerSsTable = 1000;
  private int maxCompactionThreshold = 4;
  private long commitlogFlushBytes = 1000;
  private long indexInterval = 1000;
  private boolean inMemoryColumnFamily = false;
  private long operationTimeoutInMs = 5000;
  
  public ColumnFamilyMetaData(){
    
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

  public String getImplementingClass() {
    return implementingClass;
  }

  public void setImplementingClass(String implementingClass) {
    this.implementingClass = implementingClass;
  }

  public long getIndexInterval() {
    return indexInterval;
  }

  public void setIndexInterval(long indexInterval) {
    this.indexInterval = indexInterval;
  }

  @Override
  public String toString() {
    return "ColumnFamilyMetadata [name=" + name + ", tombstoneGraceMillis=" + tombstoneGraceMillis
            + ", flushNumberOfRowKeys=" + flushNumberOfRowKeys + ", keyCachePerSsTable="
            + keyCachePerSsTable + ", maxCompactionThreshold=" + maxCompactionThreshold
            + ", commitlogFlushBytes=" + commitlogFlushBytes + "]";
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public boolean isInMemoryColumnFamily() {
    return inMemoryColumnFamily;
  }

  public void setInMemoryColumnFamily(boolean inMemoryColumnFamily) {
    this.inMemoryColumnFamily = inMemoryColumnFamily;
  }

  public long getOperationTimeoutInMs() {
    return operationTimeoutInMs;
  }

  public void setOperationTimeoutInMs(long operationTimeoutInMs) {
    this.operationTimeoutInMs = operationTimeoutInMs;
  }
  
}
