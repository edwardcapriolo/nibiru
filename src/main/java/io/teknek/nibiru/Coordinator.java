package io.teknek.nibiru;

import java.util.List;

import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  private Destination destinationLocal;
  
  public Coordinator(Server server){
    this.server = server;
  }
  
  public void init(){
    destinationLocal = new Destination();
    destinationLocal.setDestinationId(server.getServerId().getU().toString());
  }
  
  public Response handle(Message message) {
    if (SYSTEM_KEYSPACE.equals(message.getKeyspace())) {
      return null;
    }
    Keyspace keyspace = server.getKeyspaces().get(message.getKeyspace());
    List<Destination> destinations = keyspace.getKeyspaceMetadata().getRouter()
            .routesTo(message, server.getServerId(), keyspace);
    
    if (destinations.contains(destinationLocal)) {
      if (ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY.equals(message.getRequestPersonality())) {
        return handleColumnFamilyPersonality(message);
      } else {
        throw new UnsupportedOperationException(message.getRequestPersonality());
      }
    } else {
      throw new UnsupportedOperationException("We can not route messages. Yet!");
    }
  }
  
  private Response handleColumnFamilyPersonality(Message message){
    Keyspace ks = server.getKeyspaces().get(message.getKeyspace());
    if (ks == null){
      throw new RuntimeException(message.getKeyspace() + " is not found");
    }
    ColumnFamily cf = ks.getColumnFamilies().get(message.getColumnFamily());
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
