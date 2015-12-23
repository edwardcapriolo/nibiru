package io.teknek.nibiru.transport.metadata;

import java.util.Map;

public class CreateOrUpdateKeyspace extends MetaDataMessage {

  private String targetKeyspace;
  private Map<String,Object> properties;
  private boolean shouldReRoute;

  
  public CreateOrUpdateKeyspace(){
    
  }
  public String getTargetKeyspace() {
    return targetKeyspace;
  }
  public void setTargetKeyspace(String targetKeyspace) {
    this.targetKeyspace = targetKeyspace;
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

  
}
