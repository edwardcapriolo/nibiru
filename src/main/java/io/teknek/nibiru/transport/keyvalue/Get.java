package io.teknek.nibiru.transport.keyvalue;

public class Get extends KeyValueMessage{
  private String key;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }
  
}
