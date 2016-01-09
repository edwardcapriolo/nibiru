package io.teknek.nibiru.transport.keyvalue;

import io.teknek.nibiru.transport.Routable;

public class Set extends KeyValueMessage implements Routable {

  public String key;
  public String value;
  
  public String getKey() {
    return key;
  }
  
  public void setKey(String key) {
    this.key = key;
  }
  
  public String getValue() {
    return value;
  }
  
  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public String determineRoutingInformation() {
    return key;
  }
  
}
