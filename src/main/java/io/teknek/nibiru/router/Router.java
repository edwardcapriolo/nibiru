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

public interface Router {
  
  /**
   * Determine which hosts a message can be sent to. (in the future keyspace should hold a node list)
   * @param message
   * @param local
   * @param requestKeyspace
   * @return all hosts a given request can be routed to
   */
  List<Destination> routesTo(ServerId local, Keyspace requestKeyspace, ClusterMembership clusterMembership, Token token);
  
}
