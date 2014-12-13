package io.teknek.nibiru.engine;

import java.util.concurrent.atomic.AtomicReference;

import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

public class ColumnFamily {

  private ColumnFamilyMetadata columnFamilyMetadata;
  private AtomicReference<Memtable> memtable;
  private final Keyspace keyspace;
  
  public ColumnFamily(Keyspace keyspace){
    this.keyspace = keyspace;
    memtable = new AtomicReference<Memtable>(new Memtable(this));
  }

  public ColumnFamilyMetadata getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }

  public void setColumnFamilyMetadata(ColumnFamilyMetadata columnFamilyMetadata) {
    this.columnFamilyMetadata = columnFamilyMetadata;
  }

  public Memtable getMemtable() {
    return memtable.get();
  }

  public void setMemtable(Memtable memtable) {
    this.memtable.set(memtable);
  }
  
  public void put(String rowkey, String column, String value, long time){
    memtable.get().put(keyspace.getKeyspaceMetadata().getPartitioner().partition(rowkey), column, value, time, 0);
    considerFlush();
  }
  
  void considerFlush(){
    Memtable now = memtable.get();
    if (now.size() > columnFamilyMetadata.getFlushNumberOfRowKeys()){
      Memtable aNewTable = new Memtable(this);
      memtable.compareAndSet(now, aNewTable);
    }
  }
}
