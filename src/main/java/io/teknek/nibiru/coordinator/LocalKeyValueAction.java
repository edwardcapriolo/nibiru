package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class LocalKeyValueAction extends LocalAction {

  public LocalKeyValueAction(Message message, Keyspace ks, Store cf) {
    super(message, ks, cf);
    // TODO Auto-generated constructor stub
  }

  @Override
  public Response handleReqest() {
    if (columnFamily instanceof KeyValuePersonality){
      KeyValuePersonality personality = (KeyValuePersonality) columnFamily;
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
