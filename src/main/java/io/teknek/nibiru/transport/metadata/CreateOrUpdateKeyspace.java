package io.teknek.nibiru.transport.metadata;

import java.util.Map;

public class CreateOrUpdateKeyspace extends MetaDataMessage {

  private String keyspace;  
  private Map<String,Object> properties;
  private boolean shouldReRoute;

  
  public CreateOrUpdateKeyspace(){
    
  }

  public Map<String, Object> getProperties() {
    return properties;
  }
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }
  public boolean isShouldReRoute() {
    return shouldReRoute;
  }

  public void setShouldReRoute(boolean shouldReRoute2) {
   this.shouldReRoute = shouldReRoute2;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }
  
}
