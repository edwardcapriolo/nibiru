package io.teknek.nibiru.router;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Router;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;

import java.util.Arrays;
import java.util.List;

public class LocalRouter implements Router{

  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace, ClusterMembership clusterMembership) {
    Destination d = new Destination();
    d.setDestinationId(local.getU().toString());
    return Arrays.asList(d);
  }

}
