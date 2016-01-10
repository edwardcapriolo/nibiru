package io.teknek.nibiru.transport.metadata;

public class GetStoreMetaData extends MetaDataMessage {

  private String keyspace;
  private String store;

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
