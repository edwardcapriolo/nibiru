package io.teknek.nibiru.metadata;

import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyspaceMetaData {
  private String name;
  private Partitioner partitioner;
  
  //serialization
  public KeyspaceMetaData(){
    
  }
  
  public KeyspaceMetaData(String name){
    this.name = name;
    this.partitioner = new NaturalPartitioner();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

}
