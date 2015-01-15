package io.teknek.nibiru.metadata;

import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;
import io.teknek.nibiru.router.LocalRouter;
import io.teknek.nibiru.router.Router;

import java.util.Map;


public class KeyspaceMetaData {
  private String name;
  private Map<String, Object> properties;
  private String partitionerClass = NaturalPartitioner.class.getName();
  private String routerClass = LocalRouter.class.getName();
  //should the following be properties of the map?
  private transient Partitioner partitioner;
  private transient Router router;
  
  //serialization
  public KeyspaceMetaData(){
    
  }
  
  public KeyspaceMetaData(String name, Map<String,Object> properties){
    this.name = name;
    this.partitioner = new NaturalPartitioner();
    this.router = new LocalRouter();
    this.properties = properties;
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

  public String getPartitionerClass() {
    return partitionerClass;
  }

  public void setPartitionerClass(String partitionerClass) {
    this.partitionerClass = partitionerClass;
  }

  public String getRouterClass() {
    return routerClass;
  }

  public void setRouterClass(String routerClass) {
    this.routerClass = routerClass;
  }

}
