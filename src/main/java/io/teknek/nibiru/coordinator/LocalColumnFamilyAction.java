package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class LocalColumnFamilyAction extends LocalAction {
  

  public LocalColumnFamilyAction(Message message, Keyspace ks, ColumnFamily cf){
    super(message,ks,cf);
  }
  
  @Override
  public Response handleReqest() {
    if (! (this.columnFamily instanceof ColumnFamilyPersonality)){
      throw new RuntimeException("Column Family "+columnFamily.getColumnFamilyMetadata().getName() 
              + "does not support " + ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY );
    }
    ColumnFamilyPersonality personality = (ColumnFamilyPersonality) this.columnFamily;
    if (message.getPayload().get("type").equals("get")){
      Val v = personality.get(
              (String) message.getPayload().get("rowkey"),
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
  }

}
