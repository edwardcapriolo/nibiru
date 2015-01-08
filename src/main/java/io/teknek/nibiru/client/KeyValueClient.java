package io.teknek.nibiru.client;

import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class KeyValueClient extends Client {

  
  public KeyValueClient(String host, int port) {
    super(host, port);
  }

  public void put(String keyspace, String columnFamily, String key, String value) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(KeyValuePersonality.KEY_VALUE_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("key", key)
            .put("value", value).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public String get(String keyspace, String columnFamily, String key) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(KeyValuePersonality.KEY_VALUE_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "get")
            .put("key", key)
            .build();
    m.setPayload(payload);
    try {
      Response response = post(m);
      
      return (String) response.get("payload");
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
}
