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

import io.teknek.nibiru.ContactInformation;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.personality.MetaPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

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
  
  public void createOrUpdateKeyspace(String keyspace, Map<String,Object> properties, boolean isClient) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setStore(null);
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String,Object> payload = new HashMap<>();
            payload.put("type", MetaPersonality.CREATE_OR_UPDATE_KEYSPACE);
            payload.put("keyspace", keyspace);
            payload.put("properties", properties);
            if(!isClient){
              payload.put("reroute", "");
            }
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
  
  public Map<String,Object> getKeyspaceMetadata(String keyspace) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("keyspace", keyspace);
    payload.put("type", MetaPersonality.GET_KEYSPACE_METADATA);
    m.setPayload(payload);
    try {
      Response response = post(m);
      return (Map<String,Object>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
  public Map<String,Object> getStoreMetadata(String keyspace, String store) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setPersonality(MetaPersonality.META_PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("keyspace", keyspace);
    payload.put("store", store);
    payload.put("type", MetaPersonality.GET_STORE_METADATA);
    m.setPayload(payload);
    try {
      Response response = post(m);
      return (Map<String,Object>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public List<ContactInformation> getLocationForRowKey(String keyspace, String store, String rowkey) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(LocatorPersonality.PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("type", LocatorPersonality.LOCATE_ROW_KEY);
    payload.put("rowkey", rowkey);
    m.setPayload(payload);
    TypeReference tf = new TypeReference<List<ContactInformation>>() {};
    try {
      Response response = post(m);
      ObjectMapper om = new ObjectMapper();
      return (List<ContactInformation>) om.convertValue(response.get("payload"), tf);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
}
