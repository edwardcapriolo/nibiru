package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.transport.BaseMessage;

public class ColumnFamilyMessage extends BaseMessage {
  private String keyspace;
  private String store;
  
  public ColumnFamilyMessage(){}

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
