package io.teknek.nibiru.trigger;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;

public class PrintlnCoordinatorTrigger implements CoordinatorTrigger  {

  @Override
  public void exec(BaseMessage message, Response response, Server server) {
    System.out.println( "Message " + message + " response " + response);
  }

}
