package io.teknek.nibiru.router;
import java.util.List;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Router;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.transport.Message;

public class HashBasedRouter implements Router {

  public static final String MOD = "MOD";
  
  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace) {
    Number n = (Number) requestKeyspace.getKeyspaceMetadata().getProperties().get(MOD);
    int x = -1;
    if (n != null){
      x = n.intValue();
    }
    return null;
  }

}
