package io.teknek.nibiru.metadata;

import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyspaceMetadata {
  private String name;
  private Partitioner partitioner;
  private ConcurrentMap<String,ColumnFamilyMetadata> columnFamilyMetaData; 
  
  //serialization
  public KeyspaceMetadata(){
    
  }
  
  public KeyspaceMetadata(String name){
    this.name = name;
    this.partitioner = new NaturalPartitioner();
    columnFamilyMetaData = new ConcurrentHashMap<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ConcurrentMap<String, ColumnFamilyMetadata> getColumnFamilyMetaData() {
    return columnFamilyMetaData;
  }

  public void setColumnFamilyMetaData(ConcurrentMap<String, ColumnFamilyMetadata> columnFamilyMetaData) {
    this.columnFamilyMetaData = columnFamilyMetaData;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

}
