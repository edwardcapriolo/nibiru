package io.teknek.nibiru.transport.metadata;

import java.util.Map;

public class CreateOrUpdateStore extends MetaDataMessage{

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
  
}
