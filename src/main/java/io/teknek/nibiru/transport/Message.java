package io.teknek.nibiru.transport;

import java.util.Map;

public class Message {
  private String keyspace;
  private String columnFamily;
  private String requestPersonality;
  private Map<String,Object> payload;
  
  public Message(){
    
  }
  
  public String getKeyspace() {
    return keyspace;
  }
  
  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }
  
  public String getColumnFamily() {
    return columnFamily;
  }
  
  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }
  
  public Map<String, Object> getPayload() {
    return payload;
  }
  
  public void setPayload(Map<String, Object> payload) {
    this.payload = payload;
  }

  public String getRequestPersonality() {
    return requestPersonality;
  }

  public void setRequestPersonality(String requestPersonality) {
    this.requestPersonality = requestPersonality;
  }

  @Override
  public String toString() {
    return "Message [keyspace=" + keyspace + ", columnFamily=" + columnFamily
            + ", requestPersonality=" + requestPersonality + ", payload=" + payload + "]";
  }

  
}
