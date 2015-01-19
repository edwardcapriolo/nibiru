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

import io.teknek.nibiru.personality.MetaPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class MetaDataClient extends Client {

  public MetaDataClient(String host, int port) {
    super(host, port);
  }

  public void createOrUpdateKeyspace(String keyspace, Map<String,Object> properties) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setColumnFamily(null);
    m.setRequestPersonality(MetaPersonality.META_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", MetaPersonality.CREATE_OR_UPDATE_KEYSPACE)
            .put("keyspace", keyspace)
            .put("properties", properties).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public void createOrUpdateColumnFamily(String keyspace, String columnFamily,  Map<String,Object> properties) throws ClientException {
    Message m = new Message();
    m.setKeyspace("system");
    m.setColumnFamily(null);
    m.setRequestPersonality(MetaPersonality.META_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", MetaPersonality.CREATE_OR_UPDATE_COLUMN_FAMILY)
            .put("keyspace", keyspace)
            .put("columnfamily", columnFamily)
            .put("properties", properties).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  
  
  
}
