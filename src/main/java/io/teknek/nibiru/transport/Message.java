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

import io.teknek.nibiru.transport.metadata.CreateOrUpdateKeyspace;
import io.teknek.nibiru.transport.metadata.ListLiveMembersMessage;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.NAME,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type") 

    @JsonSubTypes({  
        @Type(value = ListLiveMembersMessage.class, name = "ListLiveMembersMessage"),
        @Type(value = CreateOrUpdateKeyspace.class, name = "CreateOrUpdateKeyspace"),
         })
public class Message {
  protected String keyspace;
  protected String store;
  protected String personality;
  protected Map<String,Object> payload;
  
  public Message(){
    payload = new HashMap<String,Object>();
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
  
  public void setStore(String columnFamily) {
    this.store = columnFamily;
  }
  
  public Map<String, Object> getPayload() {
    return payload;
  }
  
  public void setPayload(Map<String, Object> payload) {
    this.payload = payload;
  }

  public String getPersonality() {
    return personality;
  }

  public void setPersonality(String requestPersonality) {
    this.personality = requestPersonality;
  }

  @Override
  public String toString() {
    return "Message [keyspace=" + keyspace + ", columnFamily=" + store
            + ", personality=" + personality + ", payload=" + payload + "]";
  }

  
}
