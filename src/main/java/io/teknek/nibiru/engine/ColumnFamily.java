package io.teknek.nibiru.engine;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicReference;

import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

public class ColumnFamily {

  private ColumnFamilyMetadata columnFamilyMetadata;
  private AtomicReference<Memtable> memtable;
  private final Keyspace keyspace;
  private MemtableFlusher memtableFlusher;
  
  public ColumnFamily(Keyspace keyspace){
    this.keyspace = keyspace;
    memtable = new AtomicReference<Memtable>(new Memtable(this));
    memtableFlusher = new MemtableFlusher();
  }

  public ColumnFamilyMetadata getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }

  public void setColumnFamilyMetadata(ColumnFamilyMetadata columnFamilyMetadata) {
    this.columnFamilyMetadata = columnFamilyMetadata;
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
    return memtable.get().get(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column);
    //also search flushed memtables
    //also search sstables
  }
  
  public void delete(String rowkey, String column, long time){
    memtable.get().delete(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, time);
    considerFlush();
  }
  
  public void put(String rowkey, String column, String value, long time, long ttl){
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, ttl);
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
            && now.size() > columnFamilyMetadata.getFlushNumberOfRowKeys()){
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
}
