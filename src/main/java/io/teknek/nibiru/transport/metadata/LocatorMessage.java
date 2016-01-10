package io.teknek.nibiru.transport.metadata;

import io.teknek.nibiru.transport.BaseMessage;

public class LocatorMessage extends BaseMessage {

  private String keyspace;
  private String row;
  
  public LocatorMessage(){}

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getRow() {
    return row;
  }

  public void setRow(String row) {
    this.row = row;
  }
  
}
