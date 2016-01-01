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

import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.keyvalue.Get;
import io.teknek.nibiru.transport.keyvalue.Set;

import java.io.IOException;

public class KeyValueClient extends Client {

  
  public KeyValueClient(String host, int port) {
    super(host, port);
  }

  public void put(String keyspace, String store, String key, String value) throws ClientException {
    /*
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(columnFamily);
    m.setPersonality(KeyValuePersonality.KEY_VALUE_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", key)
            .put("value", value).build();
    m.setPayload(payload); */
    Set m = new Set();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setKey(key);
    m.setValue(value);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public String get(String keyspace, String store, String key) throws ClientException {
    Get m = new Get();
    m.setKey(key);
    m.setKeyspace(keyspace);
    m.setStore(store);
    try {
      Response response = post(m);
      return (String) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
}
