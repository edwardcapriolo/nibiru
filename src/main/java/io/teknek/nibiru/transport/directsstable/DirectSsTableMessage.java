package io.teknek.nibiru.transport.directsstable;
import io.teknek.nibiru.transport.BaseMessage;

public class DirectSsTableMessage extends BaseMessage {

  public String keyspace;
  public String store;
  private String id;
  
  public DirectSsTableMessage(){
    
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

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
  
}
