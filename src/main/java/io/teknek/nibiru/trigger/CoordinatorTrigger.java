package io.teknek.nibiru.trigger;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public interface CoordinatorTrigger {
  void exec(BaseMessage message, Response response, Server server);
}
