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

import io.teknek.nibiru.transport.keyvalue.Get;
import io.teknek.nibiru.transport.keyvalue.Set;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateKeyspace;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateStore;
import io.teknek.nibiru.transport.metadata.GetKeyspaceMetaData;
import io.teknek.nibiru.transport.metadata.GetStoreMetaData;
import io.teknek.nibiru.transport.metadata.ListKeyspaces;
import io.teknek.nibiru.transport.metadata.ListLiveMembers;
import io.teknek.nibiru.transport.metadata.ListStores;
import io.teknek.nibiru.transport.rpc.BlockingRpc;

import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.CLASS,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type") 

    @JsonSubTypes({
        //metadata
        @Type(value = ListLiveMembers.class, name = "ListLiveMembers"),
        @Type(value = CreateOrUpdateKeyspace.class, name = "CreateOrUpdateKeyspace"),
        @Type(value = GetStoreMetaData.class, name = "GetStoreMetaData"),
        @Type(value = ListStores.class, name = "ListStores"),
        @Type(value = ListKeyspaces.class, name = "ListKeyspaces"),
        @Type(value = GetKeyspaceMetaData.class, name = "GetKeyspaceMetaData"),
        @Type(value = CreateOrUpdateStore.class, name = "CreateOrUpdateStore"),
        //keyvalue
        @Type(value = Set.class, name = "Set"),
        @Type(value = Get.class, name = "Get"),
        //rpc
        @Type(value = BlockingRpc.class, name = "BlockingRpc"),
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
