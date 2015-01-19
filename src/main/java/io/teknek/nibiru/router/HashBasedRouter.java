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
import java.util.List;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;

public class HashBasedRouter implements Router {

  public static final String MOD = "MOD";
  public static final String MAP = "MAP";
  
  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace, ClusterMembership clusterMembership, Token token) {
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
