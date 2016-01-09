package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.keyvalue.Get;
import io.teknek.nibiru.transport.keyvalue.Set;

public class LocalKeyValueAction extends LocalAction {

  public LocalKeyValueAction(BaseMessage message, Keyspace ks, Store cf) {
    super(message, ks, cf);
  }

  @Override
  public Response handleReqest() {
    if (columnFamily instanceof KeyValuePersonality){
      KeyValuePersonality personality = (KeyValuePersonality) columnFamily;
      if (message instanceof Get){
        Get g = (Get) message;
        String s = personality.get(g.getKey());
        Response r = new Response();
        r.put("payload", s);
        return r;
      } else if ( message instanceof Set){
        Set s = (Set) message;
        personality.put(s.getKey(), s.getValue());
        return new Response();
      } else {
        throw new RuntimeException("Does not support this type of message");
      }
    }
    return null;
  }
}
