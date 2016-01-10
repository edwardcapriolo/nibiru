package io.teknek.nibiru.transport.metadata;

public class ListStores extends MetaDataMessage {

  private String keyspace;

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }
  
}
