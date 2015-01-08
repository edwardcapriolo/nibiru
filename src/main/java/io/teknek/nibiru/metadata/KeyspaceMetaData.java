package io.teknek.nibiru.metadata;

import io.teknek.nibiru.Router;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;
import io.teknek.nibiru.router.LocalRouter;

import java.util.HashMap;
import java.util.Map;


public class KeyspaceMetaData {
  private String name;
  private Partitioner partitioner;
  private Router router;
  private Map<String, Object> properties;
  
  //serialization
  public KeyspaceMetaData(){
    
  }
  
  public KeyspaceMetaData(String name){
    this.name = name;
    this.partitioner = new NaturalPartitioner();
    this.router = new LocalRouter();
    this.properties = new HashMap<>();
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

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

}
