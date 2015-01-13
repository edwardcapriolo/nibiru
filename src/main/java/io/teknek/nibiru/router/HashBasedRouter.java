package io.teknek.nibiru.router;
import java.util.List;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Router;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;

public class HashBasedRouter implements Router {

  public static final String MOD = "MOD";
  public static final String MAP = "MAP";
  
  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace, ClusterMembership clusterMembership) {
    Number n = (Number) requestKeyspace.getKeyspaceMetadata().getProperties().get(MOD);
    int x = -1;
    if (n != null){
      x = n.intValue();
    } 
    List<String> map = (List<String>) requestKeyspace.getKeyspaceMetadata().getProperties().get(MAP);
    Destination d = new Destination();
    
    return null;
  }

}
