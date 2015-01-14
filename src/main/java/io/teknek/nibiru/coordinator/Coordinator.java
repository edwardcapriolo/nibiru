package io.teknek.nibiru.coordinator;

import java.util.List;
import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  
  public Coordinator(Server server) {
    this.server = server;
    metaDataCoordinator = new MetaDataCoordinator(this, server.getConfiguration(),
            server.getMetaDataManager(), server.getClusterMembership());
  }
  
  public void init(){
    destinationLocal = new Destination();
    destinationLocal.setDestinationId(server.getServerId().getU().toString());
    metaDataCoordinator.init();
  }
  
  public void shutdown(){
    metaDataCoordinator.shutdown();
  }

  //ah switchboad logic
  public Response handle(Message message) {
    if (SYSTEM_KEYSPACE.equals(message.getKeyspace())) {
      return metaDataCoordinator.handleSystemMessage(message);
    }
    Keyspace keyspace = server.getKeyspaces().get(message.getKeyspace());
    if (keyspace == null){
      throw new RuntimeException(message.getKeyspace() + " is not found");
    }
    ColumnFamily columnFamily = keyspace.getColumnFamilies().get(message.getColumnFamily());
    if (columnFamily == null){
      throw new RuntimeException(message.getColumnFamily() + " is not found");
    }
    Token t = keyspace.getKeyspaceMetadata().getPartitioner().partition((String)message.getPayload().get("rowkey"));
    List<Destination> destinations = keyspace.getKeyspaceMetadata().getRouter()
            .routesTo(message, server.getServerId(), keyspace, server.getClusterMembership());
    /* This design forces every message to have a payload and a rowkey
     * Which seems like a code smell */

    long timeoutInMs = determineTimeout(columnFamily, message);
    
    if (destinations.contains(destinationLocal)) {
      if (ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY.equals(message.getRequestPersonality())) {
        return handleColumnFamilyPersonality(message, keyspace, columnFamily);
      } else if (KeyValuePersonality.KEY_VALUE_PERSONALITY.equals(message.getRequestPersonality())) { 
        return handleKeyValuePersonality(message, keyspace, columnFamily);
      } else {
        throw new UnsupportedOperationException(message.getRequestPersonality());
      }
    } else {
      throw new UnsupportedOperationException("We can not route messages. Yet!");
    }
  }
  
  private static long determineTimeout(ColumnFamily columnFamily, Message message){
    if (message.getPayload().containsKey("timeout")){
      return ((Number) message.getPayload().get("timeout")).longValue();
    } else {
      return columnFamily.getColumnFamilyMetadata().getOperationTimeoutInMs();
    }
  }
  
  private Response handleKeyValuePersonality(Message message, Keyspace ks, ColumnFamily cf){
    if (cf instanceof KeyValuePersonality){
      KeyValuePersonality personality = (KeyValuePersonality) cf;
      if (message.getPayload().get("type").equals("get")){
        String s = personality.get((String) message.getPayload().get("rowkey"));
        Response r = new Response();
        r.put("payload", s);
        return r;
      } else if (message.getPayload().get("type").equals("put")){
        personality.put((String) message.getPayload().get("rowkey"), 
                (String) message.getPayload().get("value"));
        return new Response();
      } else {
        throw new RuntimeException("Does not support this type of message");
      }
    }
    return null;
  }
  
  private Response handleColumnFamilyPersonality(Message message, Keyspace ks, ColumnFamily cf){
    if (cf instanceof ColumnFamilyPersonality){
      ColumnFamilyPersonality personality = (ColumnFamilyPersonality) cf;
      if (message.getPayload().get("type").equals("get")){
        Val v = personality.get(
                (String)message.getPayload().get("rowkey"),
                (String) message.getPayload().get("column"));
        Response r = new Response();
        r.put("payload", v);
        return r;
      } else if (message.getPayload().get("type").equals("put")) {
        Long l = ((Long) message.getPayload().get("ttl"));
        if (l == null){
          personality.put(
                (String) message.getPayload().get("rowkey"),
                (String) message.getPayload().get("column"),
                (String) message.getPayload().get("value"),
                ((Number) message.getPayload().get("time")).longValue());
          return new Response();
        } else {
          personality.put(
                  (String) message.getPayload().get("rowkey"),
                  (String) message.getPayload().get("column"),
                  (String) message.getPayload().get("value"),
                  ((Number) message.getPayload().get("time")).longValue(), l);
          return new Response();
        }
      } else if (message.getPayload().get("type").equals("delete")) { 
        personality.delete(
                (String) message.getPayload().get("rowkey"),
                (String) message.getPayload().get("column"),
                ((Number) message.getPayload().get("time")).longValue());
        return new Response();
      } else {
        throw new RuntimeException("Does not support this type of message");
      }
      
    } else {
      throw new RuntimeException("Does not support this personality");
    }
  }
}
