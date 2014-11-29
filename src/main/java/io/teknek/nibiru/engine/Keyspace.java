package io.teknek.nibiru.engine;

import io.teknek.nibiru.metadata.ColumnFamilyMetadata;
import io.teknek.nibiru.metadata.KeyspaceMetadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Keyspace {

  private KeyspaceMetadata keyspaceMetadata;
  private ConcurrentMap<String,ColumnFamily> columnFamilies;
  
  public Keyspace(){
    columnFamilies = new ConcurrentHashMap<>();
  }

  public KeyspaceMetadata getKeyspaceMetadata() {
    return keyspaceMetadata;
  }

  public void setKeyspaceMetadata(KeyspaceMetadata keyspaceMetadata) {
    this.keyspaceMetadata = keyspaceMetadata;
  }
  
  public void createColumnFamily(String name){
    ColumnFamilyMetadata cfmd = new ColumnFamilyMetadata();
    cfmd.setName(name);
    ColumnFamily cf= new ColumnFamily();
    cf.setColumnFamilyMetadata(cfmd);
    columnFamilies.put(name, cf);
  }

  public ConcurrentMap<String, ColumnFamily> getColumnFamilies() {
    return columnFamilies;
  }

  public void setColumnFamilies(ConcurrentMap<String, ColumnFamily> columnFamilies) {
    this.columnFamilies = columnFamilies;
  }
  
  public Token createToken(String rowkey){
    return keyspaceMetadata.getPartitioner().partition(rowkey);
  }
}
