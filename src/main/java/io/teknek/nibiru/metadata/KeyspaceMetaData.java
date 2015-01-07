package io.teknek.nibiru.metadata;

import io.teknek.nibiru.LocalRouter;
import io.teknek.nibiru.Router;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyspaceMetaData {
  private String name;
  private Partitioner partitioner;
  private Router router;
  
  //serialization
  public KeyspaceMetaData(){
    
  }
  
  public KeyspaceMetaData(String name){
    this.name = name;
    this.partitioner = new NaturalPartitioner();
    this.router = new LocalRouter();
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

  public Router getRouter() {
    return router;
  }

  public void setRouter(Router router) {
    this.router = router;
  }

}
