/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
