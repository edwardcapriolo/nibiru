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
package io.teknek.nibiru.client;

import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.personality.MetaPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaDataClient extends Client {

  public MetaDataClient(String host, int port) {
    super(host, port);
  }

  public List<ClusterMember> getLiveMembers() throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String,Object> payload = new HashMap<>();
    payload.put("type", MetaPersonality.LIST_LIVE_MEMBERS);
    m.setPayload(payload);
    try {
      Response response = post(m); 
      List<Map> payloadAsMap = (List<Map>) response.get("payload");
      List<ClusterMember> res = new ArrayList<>(payloadAsMap.size());
      for (Map entry : payloadAsMap){
        res.add(MAPPER.convertValue(entry, ClusterMember.class));
      }
      return res;
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public void createOrUpdateKeyspace(String keyspace, Map<String,Object> properties) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setStore(null);
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String,Object> payload = new HashMap<>();
            payload.put("type", MetaPersonality.CREATE_OR_UPDATE_KEYSPACE);
            payload.put("keyspace", keyspace);
            payload.putAll(properties);
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public void createOrUpdateStore(String keyspace, String store,  Map<String,Object> properties) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setStore(null);
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", MetaPersonality.CREATE_OR_UPDATE_STORE);
    payload.put("keyspace", keyspace);
    payload.put("store", store);
    payload.putAll(properties);
    m.setPayload(payload);
    try {
      Response response = post(m);
      System.out.println(response);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public Collection<String> listKeyspaces() throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", MetaPersonality.LIST_KEYSPACES);
    m.setPayload(payload);
    try {
      Response response = post(m);
      return (Collection<String>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public Collection<String> listStores(String keyspace) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("keyspace", keyspace);
    payload.put("type", MetaPersonality.LIST_STORES);
    m.setPayload(payload);
    try {
      Response response = post(m);
      return (Collection<String>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
  public static void main (String [] args) throws ClientException {
    MetaDataClient c = new MetaDataClient("127.0.0.1", 7070);
    System.out.println(c.getLiveMembers());
    
  }
  
  
}
