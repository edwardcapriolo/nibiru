package io.teknek.nibiru;

import io.teknek.nibiru.transport.Message;

import java.util.Arrays;
import java.util.List;

public class LocalRouter implements Router{

  @Override
  public List<Destination> routesTo(Message message, ServerId local, Keyspace requestKeyspace) {
    Destination d = new Destination();
    d.setDestinationId(local.getU().toString());
    return Arrays.asList(d);
  }

}
