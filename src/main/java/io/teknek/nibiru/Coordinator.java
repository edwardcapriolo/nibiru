package io.teknek.nibiru;

import org.codehaus.jackson.map.ObjectMapper;

import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  
  public Coordinator(Server server){
    this.server = server;
  }
  
  public Response handle(Message message){
    if (SYSTEM_KEYSPACE.equals(message.getKeyspace())){
      return null;
    }
    if (ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY.equals(message.getRequestPersonality())){
      return handleColumnFamilyPersonality(message);
    } else {
      throw new UnsupportedOperationException(message.getRequestPersonality());
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
        personality.put(
                (String) message.getPayload().get("rowkey"),
                (String) message.getPayload().get("column"),
                (String) message.getPayload().get("value"),
                ((Number) message.getPayload().get("time")).longValue());
        return new Response();
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
