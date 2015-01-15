package io.teknek.nibiru.router;

import java.util.List;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;

public interface Router {
  
  /**
   * Determine which hosts a message can be sent to. (in the future keyspace should hold a node list)
   * @param message
   * @param local
   * @param requestKeyspace
   * @return all hosts a given request can be routed to
   */
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace, ClusterMembership clusterMembership, Token token);
    /*
    String rk = (String) m.getPayload().get("rowkey");//todo t
    Token partition = keyspace.getKeyspaceMetadata().getPartitioner().partition(rk);
    //List[uuid/host/{token}] keyspace.getTopology()
    return null;
    */
  
}
