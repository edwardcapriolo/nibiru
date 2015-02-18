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
package io.teknek.nibiru.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
 
import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.TombstoneValue;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;

public class DefaultColumnFamily extends ColumnFamily implements ColumnFamilyPersonality {

  private AtomicReference<Memtable> memtable;
  private MemtableFlusher memtableFlusher;
  private Set<SsTable> sstable = new ConcurrentSkipListSet<>();
  
  public DefaultColumnFamily(Keyspace keyspace, ColumnFamilyMetaData cfmd){
    super(keyspace, cfmd);
    //It would be nice to move this into init but some things are dependent
    CommitLog commitLog = new CommitLog(this);
    try {
      commitLog.open();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    memtable = new AtomicReference<Memtable>(new Memtable(this, commitLog));
    memtableFlusher = new MemtableFlusher(this);
    memtableFlusher.start();
  }

  public void init() throws IOException {
    for(File ssTable: keyspace.getConfiguration().getDataDirectory().listFiles()){
      String [] parts = ssTable.getName().split("\\.");
      if (parts.length == 2){
        if ("ss".equalsIgnoreCase(parts[1])){
          String id = parts[0];
          SsTable toOpen = new SsTable(this);
          toOpen.open(id, keyspace.getConfiguration());
          sstable.add(toOpen);
        }
      }
    }
    
    for(File commitlog: CommitLog.getCommitLogDirectoryForColumnFamily(this).listFiles()){
      String [] parts = commitlog.getName().split("\\.");
      if (parts.length == 2){
        if (CommitLog.EXTENSION.equalsIgnoreCase(parts[1])){
          processCommitLog(parts[0]);
        }
      }
    }
  }
  
  void processCommitLog(String id) throws IOException {
    CommitLogReader r = new CommitLogReader(id, this);
    r.open();
    Token t;
    while ((t = r.getNextToken()) != null){
      SortedMap<AtomKey, AtomValue> x = r.readColumns();
      for (Entry<AtomKey, AtomValue> col: x.entrySet()){
        if (col.getKey() instanceof RowTombstoneKey){
          TombstoneValue v = (TombstoneValue) col.getValue();
          memtable.get().delete(t, v.getTime());
        } else if (col.getKey() instanceof ColumnKey){
          ColumnKey ck = (ColumnKey) col.getKey();
          if (col.getValue() instanceof ColumnValue){
            ColumnValue cv = (ColumnValue) col.getValue();
            memtable.get().put(t, ck.getColumn(), cv.getValue(), cv.getTime(), cv.getTtl());
          } else if (col.getValue() instanceof TombstoneValue){
            memtable.get().delete(t, ck.getColumn(), col.getValue().getTime());
          } else {
            throw new RuntimeException("processing commit log "+id);
          }
        } else {
          throw new RuntimeException("processing commit log "+id);
        }
      }
    }
    r.close();
    r.delete();
  }
  
  public void shutdown(){
    getMemtableFlusher().setGoOn(false);
    for (SsTable s: sstable){
      try {
        s.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    //TODO should probably flush here
    try {
      memtable.get().getCommitLog().close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public ColumnFamilyMetaData getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }
  
  @Deprecated
  public Memtable getMemtable() {
    return memtable.get();
  }

  @Deprecated
  public void setMemtable(Memtable memtable) {
    this.memtable.set(memtable);
  }
  
  public static AtomValue applyRules(AtomValue lastValue, AtomValue thisValue){
    if (thisValue == null){
      return lastValue;
    }
    if (thisValue != null && lastValue == null){
      return thisValue;
    }
    if (thisValue instanceof TombstoneValue && 
            thisValue.getTime() >= lastValue.getTime()){
      return thisValue;
    }
    if (lastValue instanceof TombstoneValue && 
            lastValue.getTime() >= thisValue.getTime()){
      return lastValue;
    }
    
    if (thisValue instanceof ColumnValue && lastValue instanceof ColumnValue){
      if (thisValue.getTime() == lastValue.getTime()){ 
        if ( ((ColumnValue) thisValue).getValue().compareTo(((ColumnValue) lastValue).getValue() ) > 0){
          return thisValue;  
        } else {
          return lastValue;
        }
      } else if (thisValue.getTime() > lastValue.getTime()){
        return thisValue;  
      } else {
        return lastValue;
      }
    } 
    
    throw new IllegalArgumentException ( "comparing " + thisValue + " " + lastValue);
  }
  
  public AtomValue get(String rowkey, String column){
    AtomValue lastValue = memtable.get().get(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column);
    for (Memtable m: memtableFlusher.getMemtables()){
      AtomValue thisValue = m.get(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column);
      lastValue = applyRules(lastValue, thisValue);
    }
    Token token = keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey);
    for (SsTable sstable: this.getSstable()){
      AtomValue thisValue = null;
      try {
        thisValue = sstable.get(token, column);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      lastValue = applyRules(lastValue, thisValue);
    }
    return lastValue;
  }
  
  public void delete(String rowkey, String column, long time){
    memtable.get().delete(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, time);
    //commit log
    considerFlush();
  }
  
  public void put(String rowkey, String column, String value, long time, long ttl){
    try {
      memtable.get().getCommitLog().write(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), new ColumnKey(column), value, time, ttl);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, ttl);
    considerFlush();
  }
    
  public void put(String rowkey, String column, String value, long time){
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, 0);
    //commit log
    considerFlush();
  }
  
  void considerFlush(){
    Memtable now = memtable.get();
    if (columnFamilyMetadata.getFlushNumberOfRowKeys() != 0 
            && now.size() >= columnFamilyMetadata.getFlushNumberOfRowKeys()){
      CommitLog commitLog = new CommitLog(this);
      try {
        commitLog.open();
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }
      Memtable aNewTable = new Memtable(this, commitLog); 
      boolean success = memtableFlusher.add(now);
      if (success){
        boolean swap = memtable.compareAndSet(now, aNewTable);
        if (!swap){
          throw new RuntimeException("race detected");
        }        
      }
    }
  }

  public Keyspace getKeyspace() {
    return keyspace;
  }

  public Set<SsTable> getSstable() {
    return sstable;
  }

  public MemtableFlusher getMemtableFlusher() {
    return memtableFlusher;
  }
  
  public SortedMap<AtomKey, AtomValue>  slice(String rowkey, String start, String end){
    Token t = keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey);
    SortedMap<AtomKey, AtomValue> fromMemtable = memtable.get().slice(t, start, end);
    for (SsTable table: this.sstable){
      try {
        Map<AtomKey, AtomValue> fromSs = table.slice(t, start, end);
        for (Entry<AtomKey, AtomValue> each: fromSs.entrySet()){
          if (!fromMemtable.containsKey(each.getKey())){
            fromMemtable.put(each.getKey(), each.getValue());
          } else {
            //TODO use better rules that consider tombstones
            AtomValue current = fromMemtable.get(each.getKey());
            if (each.getValue().getTime() > current.getTime()){
              fromMemtable.put(each.getKey(), each.getValue());
            }
          }
        }
      } catch (IOException e) {
        throw new RuntimeException (e);
      }
    }
    return fromMemtable;
  }
}
