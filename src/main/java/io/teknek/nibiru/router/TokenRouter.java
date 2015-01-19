package io.teknek.nibiru.router;

import java.util.ArrayList;
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
  public static final String REPLICATION_FACTOR = "replication_factor";
  
  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace,
          ClusterMembership clusterMembership, Token token) {
    TreeMap<String, String> tokenMap = (TreeMap<String, String>) requestKeyspace
            .getKeyspaceMetadata().getProperties().get(TOKEN_MAP_KEY);
    Integer replicationFactor = (Integer) requestKeyspace
            .getKeyspaceMetadata().getProperties().get(REPLICATION_FACTOR);
    int rf = 1;
    if (replicationFactor != null){
      rf = replicationFactor;
    }
    if (rf > tokenMap.size()) {
      throw new IllegalArgumentException(
              "Replication factor specified was larger than token map size");
    }
    List<Destination> destinations = new ArrayList<Destination>();
    String key = token.getToken();
    key = tokenMap.ceilingKey(key);
    if (key == null){
      key = tokenMap.firstKey();
    }
    destinations.add(new Destination(key));
    for (int i = 1; i < rf; i++) {
      key = tokenMap.higherKey(key);
      if (key == null) {
        key = tokenMap.firstKey();
      }
      destinations.add(new Destination(key));
    }
    return destinations;
  }

}
/*
/*
String serverId = tokenMap.ceilingKey(token.getToken());
if (serverId == null){
  serverId = tokenMap.firstKey();
}
Destination d = new Destination();
d.setDestinationId(serverId);
List<Destination> x = Arrays.asList(d);
return x;*/
