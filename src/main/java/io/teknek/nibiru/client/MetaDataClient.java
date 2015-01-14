package io.teknek.nibiru.client;

import io.teknek.nibiru.personality.KeyValuePersonality;
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
  
}
