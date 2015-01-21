/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru.router;

import java.util.ArrayList;
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
