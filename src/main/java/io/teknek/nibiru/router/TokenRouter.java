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
import java.util.Map;
import java.util.TreeMap;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.cluster.ClusterMembership;

public class TokenRouter implements Router {

  public static final String TOKEN_MAP_KEY = "token_map";
  public static final String REPLICATION_FACTOR = "replication_factor";
  
  @SuppressWarnings("unchecked")
  private TreeMap<String,String> getTokenMap(Keyspace requestKeyspace){
     Map<String,String> s = (Map<String, String>) requestKeyspace
    .getKeyspaceMetaData().getProperties().get(TOKEN_MAP_KEY);
    return new TreeMap<String,String>(s);
  }
  
  private int getReplicationFactor(Keyspace requestKeyspace){
    Integer replicationFactor = (Integer) requestKeyspace
            .getKeyspaceMetaData().getProperties().get(REPLICATION_FACTOR);
    int rf = 1;
    if (replicationFactor != null){
      rf = replicationFactor;
    }
    return rf;
  }
  
  @Override
  public List<Destination> routesTo(ServerId local, Keyspace requestKeyspace,
          ClusterMembership clusterMembership, Token token) {
    TreeMap<String, String> tokenMap = getTokenMap(requestKeyspace);
    int rf = getReplicationFactor(requestKeyspace);
    if (rf > tokenMap.size()) {
      throw new IllegalArgumentException("Replication factor > than token map size");
    }
    List<Destination> destinations = new ArrayList<Destination>();
    Map.Entry<String,String> ceilingEntry = tokenMap.ceilingEntry(token.getToken());
    if (ceilingEntry == null){
      ceilingEntry = tokenMap.firstEntry();
    }
    destinations.add(new Destination(ceilingEntry.getValue()));
    for (int i = 1; i < rf; i++) {
      ceilingEntry = tokenMap.higherEntry(ceilingEntry.getKey());
      if (ceilingEntry == null) {
        ceilingEntry = tokenMap.firstEntry();
      }
      destinations.add(new Destination(ceilingEntry.getValue()));
    }
    return destinations;
  }

}
