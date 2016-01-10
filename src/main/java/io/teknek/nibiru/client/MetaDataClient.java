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
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateKeyspace;
import io.teknek.nibiru.transport.metadata.CreateOrUpdateStore;
import io.teknek.nibiru.transport.metadata.GetKeyspaceMetaData;
import io.teknek.nibiru.transport.metadata.GetStoreMetaData;
import io.teknek.nibiru.transport.metadata.ListKeyspaces;
import io.teknek.nibiru.transport.metadata.ListStores;
import io.teknek.nibiru.transport.metadata.LocatorMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class MetaDataClient extends Client {

  @Deprecated
  public MetaDataClient(String host, int port) {
    super(host, port);
  }
  
  public MetaDataClient(String host, int port, int c, int s) {
    super(host, port, c, s);
  }

  public List<ClusterMember> getLiveMembers() throws ClientException {
    BaseMessage m = new io.teknek.nibiru.transport.metadata.ListLiveMembers();
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
    CreateOrUpdateKeyspace k = new CreateOrUpdateKeyspace();
    k.setKeyspace(keyspace);
    if(isClient){
      k.setShouldReRoute(true);
    }
    k.setProperties(properties);
    try {
      Response response = post(k);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public void createOrUpdateStore(String keyspace, String store,  Map<String,Object> properties, boolean isClient) throws ClientException {
    CreateOrUpdateStore m = new CreateOrUpdateStore();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setProperties(properties);
    if (isClient){
      m.setShouldReroute(true);
    }
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public Collection<String> listKeyspaces() throws ClientException {
    ListKeyspaces m = new ListKeyspaces();
    try {
      Response response = post(m);
      return (Collection<String>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public Collection<String> listStores(String keyspace) throws ClientException {
   ListStores m = new ListStores();
   m.setKeyspace(keyspace);
    try {
      Response response = post(m);
      return (Collection<String>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public Map<String,Object> getKeyspaceMetadata(String keyspace) throws ClientException {
    GetKeyspaceMetaData  m = new GetKeyspaceMetaData();
    m.setKeyspace(keyspace);
    try {
      Response response = post(m);
      return (Map<String,Object>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
  public Map<String,Object> getStoreMetadata(String keyspace, String store) throws ClientException {
    GetStoreMetaData m = new GetStoreMetaData();
    m.setKeyspace(keyspace);
    m.setStore(store);
    try {
      Response response = post(m);
      return (Map<String,Object>) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public List<ContactInformation> getLocationForRowKey(String keyspace, String store, String rowkey) throws ClientException{
    LocatorMessage m = new LocatorMessage();
    m.setKeyspace(keyspace);
    m.setRow(rowkey);
    TypeReference<List<ContactInformation>> tf = new TypeReference<List<ContactInformation>>() {};
    try {
      Response response = post(m);
      ObjectMapper om = new ObjectMapper();
      return (List<ContactInformation>) om.convertValue(response.get("payload"), tf);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
}
