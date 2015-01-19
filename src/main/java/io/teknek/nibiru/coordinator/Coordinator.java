package io.teknek.nibiru.coordinator;

import java.util.List;
import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  //TODO. this needs to be a per column family value
  private final EventualCoordinator eventualCoordinator;
  
  public Coordinator(Server server) {
    this.server = server;
    metaDataCoordinator = new MetaDataCoordinator(this, server.getConfiguration(),
            server.getMetaDataManager(), server.getClusterMembership());
    eventualCoordinator = new EventualCoordinator(server.getClusterMembership());
  }
  
  public void init(){
    destinationLocal = new Destination();
    destinationLocal.setDestinationId(server.getServerId().getU().toString());
    metaDataCoordinator.init();
    eventualCoordinator.init();
  }
  
  public void shutdown(){
    metaDataCoordinator.shutdown();
    eventualCoordinator.shutdown();
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
    Token token = keyspace.getKeyspaceMetadata().getPartitioner().partition((String) message.getPayload().get("rowkey"));
    List<Destination> destinations = keyspace.getKeyspaceMetadata().getRouter()
            .routesTo(message, server.getServerId(), keyspace, server.getClusterMembership(), token);
    long timeoutInMs = determineTimeout(columnFamily, message);
    Consistency consistency = determinteConsistency(message);
    if (ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY.equals(message.getRequestPersonality())) {
      LocalAction action = new LocalColumnFamilyAction(message, keyspace, columnFamily);
      //return action.handleReqest();
      return eventualCoordinator.handleMessage(token, message, destinations, 
              timeoutInMs, consistency, destinationLocal,action);
    } else if (KeyValuePersonality.KEY_VALUE_PERSONALITY.equals(message.getRequestPersonality())) { 
      return handleKeyValuePersonality(message, keyspace, columnFamily);
    } else {
      throw new UnsupportedOperationException(message.getRequestPersonality());
    }

  }
  
  private static long determineTimeout(ColumnFamily columnFamily, Message message){
    if (message.getPayload().containsKey("timeout")){
      return ((Number) message.getPayload().get("timeout")).longValue();
    } else {
      return columnFamily.getColumnFamilyMetadata().getOperationTimeoutInMs();
    }
  }
  
  private static Consistency determinteConsistency(Message message){
    return new Consistency().withLevel(ConsistencyLevel.IMPLIED);
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
  
}
