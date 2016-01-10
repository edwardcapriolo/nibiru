package io.teknek.nibiru.transport.metadata;

import java.util.Map;

public class CreateOrUpdateStore extends MetaDataMessage{

  private String keyspace;
  private String store;
  private Map<String,Object> properties;
  private boolean shouldReroute;

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  public boolean isShouldReroute() {
    return shouldReroute;
  }

  public void setShouldReroute(boolean shouldReroute) {
    this.shouldReroute = shouldReroute;
  }
  
  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getStore() {
    return store;
  }

  public void setStore(String store) {
    this.store = store;
  }
  
}
