package io.teknek.nibiru.engine;

import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

public class ColumnFamily {

  private ColumnFamilyMetadata columnFamilyMetadata;
  private Memtable memtable;
  
  public ColumnFamily(){
    memtable = new Memtable();
  }

  public ColumnFamilyMetadata getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }

  public void setColumnFamilyMetadata(ColumnFamilyMetadata columnFamilyMetadata) {
    this.columnFamilyMetadata = columnFamilyMetadata;
  }

  public Memtable getMemtable() {
    return memtable;
  }

  public void setMemtable(Memtable memtable) {
    this.memtable = memtable;
  }
  
}
