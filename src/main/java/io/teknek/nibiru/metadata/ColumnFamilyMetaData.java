/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru.metadata;

import java.util.Map;

public class ColumnFamilyMetaData {
  public static final String IMPLEMENTING_CLASS = "implementing_class"; 
  
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
  private boolean enableHints = true;
  
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

  public boolean isEnableHints() {
    return enableHints;
  }

  public void setEnableHints(boolean enableHints) {
    this.enableHints = enableHints;
  }
  
  
  
}
