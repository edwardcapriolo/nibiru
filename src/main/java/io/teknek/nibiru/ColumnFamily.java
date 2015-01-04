package io.teknek.nibiru;

import java.io.IOException;

import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

public abstract class ColumnFamily {
  protected final Keyspace keyspace;
  protected final ColumnFamilyMetaData columnFamilyMetadata;
  
  public ColumnFamily(Keyspace keyspace, ColumnFamilyMetaData cfmd){
    this.keyspace = keyspace;
    this.columnFamilyMetadata = cfmd;
  }

  public ColumnFamilyMetaData getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }
  
  public Keyspace getKeyspace() {
    return keyspace;
  }

  public abstract void init() throws IOException;

  public abstract void shutdown() throws IOException;

}
