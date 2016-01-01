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

import io.teknek.nibiru.TriggerDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StoreMetaData {
  public static final String IMPLEMENTING_CLASS = "implementing_class"; 
  
  //TODO properties are column family specific we could refactor the metadata so that 
  //these constants are closer to the table type
  public static final String ENABLE_HINTS = "enable_hints";
  public static final String FLUSH_NUMBER_OF_ROW_KEYS = "flush_number_of_row_keys";
  public static final String KEY_CACHE_PER_SSTABLE = "key_cache_per_sstable";
  public static final String MAX_COMPACTION_THRESHOLD = "max_compaction_threshold";
  public static final String COMMITLOG_FLUSH_BYTES = "commit_log_flush_bytes";
  public static final String INDEX_INTERVAL = "index_interval";
  public static final String IN_MEMORY_CF = "in_memory_cf";
  public static final String OPERATION_TIMEOUT_IN_MS = "operation_timeout_ms";
  public static final String TOMBSTONE_GRACE_MS = "tombstone_grace_ms";
  public static final String COORDINATOR_TRIGGERS = "coordinator_triggers";
  public static final String MEMTABLE_CLASS = "memtable_class";
  
  private String name;
  private ConcurrentMap<String,Object> properties;
  
  public StoreMetaData(){
    properties = new ConcurrentHashMap<String,Object>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @SuppressWarnings("unchecked")
  public List<TriggerDefinition> getCoordinatorTriggers(){
    List<TriggerDefinition> def = (List<TriggerDefinition>) properties.get(COORDINATOR_TRIGGERS);
    if (def == null){
      properties.putIfAbsent(COORDINATOR_TRIGGERS, new ArrayList<>());
      return (List<TriggerDefinition>) properties.get(COORDINATOR_TRIGGERS);
    } else {
      return def;
    }
  }
  
  public void setCoordinatorTriggers(List<TriggerDefinition> triggers){
    properties.put(COORDINATOR_TRIGGERS, triggers);
  }
  
  public long getTombstoneGraceMillis() {
    Number res = (Number) properties.get(TOMBSTONE_GRACE_MS);
    if (res == null){
      return 1000 * 60 * 60 * 24 * 3;
    } 
    return res.longValue();
  }

  public void setTombstoneGraceMillis(long tombstoneGraceTime) {
    properties.put(TOMBSTONE_GRACE_MS, tombstoneGraceTime);
  }

  public int getFlushNumberOfRowKeys() {
    Number res = (Number) properties.get(FLUSH_NUMBER_OF_ROW_KEYS);
    if (res == null){
      return 10000;
    } 
    return res.intValue();
  }

  public void setFlushNumberOfRowKeys(int flushNumberOfRowKeys) {
    properties.put(FLUSH_NUMBER_OF_ROW_KEYS, flushNumberOfRowKeys);
  }

  public int getKeyCachePerSsTable() {
    Number res = (Number) properties.get(KEY_CACHE_PER_SSTABLE);
    if (res == null){
      return 1000;
    } 
    return res.intValue();
  }

  public void setKeyCachePerSsTable(int keyCachePerSsTable) {
    properties.put(KEY_CACHE_PER_SSTABLE, keyCachePerSsTable);
  }

  public int getMaxCompactionThreshold() {
    Number res = (Number) properties.get(MAX_COMPACTION_THRESHOLD);
    if (res == null){
      return 4;
    } 
    return res.intValue();
  }

  public void setMaxCompactionThreshold(int maxCompactionThreshold) {
    properties.put(MAX_COMPACTION_THRESHOLD, maxCompactionThreshold);
  }

  public long getCommitlogFlushBytes() {
    Number res = (Number) properties.get(COMMITLOG_FLUSH_BYTES);
    if (res == null){
      return 1000;
    } 
    return res.intValue();
  }

  public void setCommitlogFlushBytes(long commitlogFlushBytes) {
    properties.put(COMMITLOG_FLUSH_BYTES, commitlogFlushBytes);
  }

  public String getImplementingClass() {
    return (String) properties.get(StoreMetaData.IMPLEMENTING_CLASS);
  }

  public void setImplementingClass(String implementingClass) {
    properties.put(IMPLEMENTING_CLASS, implementingClass);
  }
  
  
  public String getMemtableClass() {
    return (String) properties.get(StoreMetaData.MEMTABLE_CLASS);
  }

  public void setMemtableClass(String memtableClass) {
    properties.put(MEMTABLE_CLASS, memtableClass);
  }
  

  public long getIndexInterval() {
    Number res = (Number) properties.get(INDEX_INTERVAL);
    if (res == null){
      return 1000;
    } 
    return res.intValue();
  }

  public void setIndexInterval(long indexInterval) {
    properties.put(INDEX_INTERVAL, indexInterval);
  }


  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = new ConcurrentHashMap<>(properties);
  }

  public boolean isInMemoryColumnFamily() {
    Boolean res = (Boolean) properties.get(IN_MEMORY_CF);
    if (res == null){
      return false;
    } 
    return res.booleanValue();
  }

  public void setInMemoryColumnFamily(boolean inMemoryColumnFamily) {
    properties.put(IN_MEMORY_CF, inMemoryColumnFamily);
  }

  public long getOperationTimeoutInMs() {
    Number res = (Number) properties.get(OPERATION_TIMEOUT_IN_MS);
    if (res == null){
      return 5000;
    } 
    return res.intValue();
  }

  public void setOperationTimeoutInMs(long operationTimeoutInMs) {
    properties.put(OPERATION_TIMEOUT_IN_MS, operationTimeoutInMs);
  }

  public boolean isEnableHints() {
    Boolean res = (Boolean) properties.get(ENABLE_HINTS);
    if (res == null){
      return true;
    } 
    return res.booleanValue();
  }

  public void setEnableHints(boolean enableHints) {
    properties.put(ENABLE_HINTS, enableHints);
  }

  @Override
  public String toString() {
    return "StoreMetaData [name=" + name + ", properties=" + properties + "]";
  }
  
}