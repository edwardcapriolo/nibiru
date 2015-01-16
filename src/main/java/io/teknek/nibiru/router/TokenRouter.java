package io.teknek.nibiru.router;

import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message; 

public class TokenRouter implements Router {

  public static final String TOKEN_MAP_KEY = "token_map";
  
  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace,
          ClusterMembership clusterMembership, Token token) {
    TreeMap<String, String> tokenMap = (TreeMap<String, String>) requestKeyspace
            .getKeyspaceMetadata().getProperties().get(TOKEN_MAP_KEY);
    String serverId = tokenMap.ceilingKey(token.getToken());
    if (serverId == null){
      serverId = tokenMap.firstKey();
    }
    Destination d = new Destination();
    d.setDestinationId(serverId);
    List<Destination> x = Arrays.asList(d);
    return x;
  }

}
