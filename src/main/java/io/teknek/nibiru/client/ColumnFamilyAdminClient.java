package io.teknek.nibiru.client;

import io.teknek.nibiru.personality.ColumnFamilyAdminPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamilyadmin.CleanupMessage;

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
    CleanupMessage m = new CleanupMessage();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
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
