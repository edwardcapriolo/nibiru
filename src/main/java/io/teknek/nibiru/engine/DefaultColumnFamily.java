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
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
 
import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
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
      SortedMap<String,Val> x = r.readColumns();
      for (Map.Entry<String,Val> col: x.entrySet()){
        //note this changes the create time which could effect ttl. Need new constructor.
        memtable.get().put(t, col.getKey(), col.getValue().getValue(), col.getValue().getTime(), col.getValue().getTtl());
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
  
  public Val get(String rowkey, String column){
    Val v = memtable.get().get(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column);
    for (Memtable m: memtableFlusher.getMemtables()){
      Val x = m.get(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column);
      if (x == null){
        continue;
      }
      if (v == null){
        v = x;
        continue;
      }
      if (x.getTime() > v.getTime()){
        v = x;
      } else if (x.getTime() == v.getTime() && "".equals(x.getValue())){
        v = null;
      } else if (x.getTime() == v.getTime()){
        int compare = x.getValue().compareTo(v.getValue());
        if (compare == 0){
          //v is unchanged          
        } else if (compare > 0){
          v = x;
        } else if (compare < 0){
          //v is unchanged
        }
      }
    }
    Token token = keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey);
    for (SsTable sstable: this.getSstable()){
      Val x = null;
      try {
        x = sstable.get(token, column);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (x == null){
        continue;
      }
      if (v == null){
        v = x;
        continue;
      }
      if (x.getTime() > v.getTime()){
        v = x;
      } else if (x.getTime() == v.getTime() && "".equals(x.getValue())){
        v = null;
      } else if (x.getTime() == v.getTime()){
        int compare = x.getValue().compareTo(v.getValue());
        if (compare == 0){
          //v is unchanged          
        } else if (compare > 0){
          v = x;
        } else if (compare < 0){
          //v is unchanged
        }
      }
    }
    return v;
  }
  
  public void delete(String rowkey, String column, long time){
    memtable.get().delete(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, time);
    //commit log
    considerFlush();
  }
  
  public void put(String rowkey, String column, String value, long time, long ttl){
    try {
      memtable.get().getCommitLog().write(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, ttl);
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
  
  public ConcurrentNavigableMap<String, Val>  slice(String rowkey, String start, String end){
    Token t = keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey);
    ConcurrentNavigableMap<String, Val> fromMemtable = memtable.get().slice(t, start, end);
    for (SsTable table: this.sstable){
      try {
        Map<String,Val> each = table.slice(t, start, end);
        
      } catch (IOException e) {
        throw new RuntimeException (e);
      }
    }
    return null;
  }
}
/*
 *   public ConcurrentNavigableMap<String, Val>  slice(String rowkey, String start, String end){
    return memtable.get().slice(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), start, end);
    //also search flushed memtables
    //also search sstables
  } */
