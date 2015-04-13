package io.teknek.nibiru.client;

import io.teknek.nibiru.personality.ColumnFamilyAdminPersonality;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public class ColumnFamilyAdminClient {

  private final Client client;
  
  public ColumnFamilyAdminClient(String host, int port){
    client = new Client(host, port);
  }
  
  public ColumnFamilyAdminClient(Client client){
    this.client = client;
  }
  
  public void cleanup(String keyspace, String columnFamily) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(columnFamily);
    m.setPersonality(ColumnFamilyAdminPersonality.PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", ColumnFamilyAdminPersonality.CLEANUP).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public void shutdown(){
    client.shutdown();
  }
}
