package io.teknek.nibiru.transport.keyvalue;

import io.teknek.nibiru.transport.Routable;

public class Get extends KeyValueMessage implements Routable{
  private String key;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  @Override
  public String determineRoutingInformation() {
    return key;
  }
  
}
