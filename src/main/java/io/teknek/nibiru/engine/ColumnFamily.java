package io.teknek.nibiru.engine;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;

import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

public class ColumnFamily {

  private final ColumnFamilyMetadata columnFamilyMetadata;
  private AtomicReference<Memtable> memtable;
  private final Keyspace keyspace;
  private MemtableFlusher memtableFlusher;
  private Set<SsTable> sstable = new ConcurrentSkipListSet<>();
  private CommitLog commitLog;
  
  public ColumnFamily(Keyspace keyspace, ColumnFamilyMetadata cfmd){
    this.keyspace = keyspace;
    this.columnFamilyMetadata = cfmd;
    memtable = new AtomicReference<Memtable>(new Memtable(this));
    memtableFlusher = new MemtableFlusher(this);
    memtableFlusher.start();
    commitLog = new CommitLog(this);
    commitLog.getAssociatedMemtables().add(memtable.get());
    commitLog.start();
  }

  public ColumnFamilyMetadata getColumnFamilyMetadata() {
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
    for (SsTable sstable: this.getSstable()){
      Val x = null;
      try {
        x = sstable.get(rowkey, column);
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
    considerFlush();
  }
  
  public void put(String rowkey, String column, String value, long time, long ttl){
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, ttl);
    //commitLog.get().write(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, ttl);
    considerFlush();
  }
  
  public ConcurrentNavigableMap<String, Val>  slice(String rowkey, String start, String end){
    return memtable.get().slice(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), start, end);
    //also search flushed memtables
    //also search sstables
  }
  
  public void put(String rowkey, String column, String value, long time){
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, 0);
    considerFlush();
  }
  
  void considerFlush(){
    Memtable now = memtable.get();
    if (columnFamilyMetadata.getFlushNumberOfRowKeys() != 0 
            && now.size() >= columnFamilyMetadata.getFlushNumberOfRowKeys()){
      Memtable aNewTable = new Memtable(this); 
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
  
  
}
