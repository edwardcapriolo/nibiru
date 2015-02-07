package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.concurrent.Callable;

public class RemoteMessageCallable extends CompletableCallable implements Callable<Response> {

  private final Client client;
  private final Destination destination;
  private final Message message;
  
  public RemoteMessageCallable(Client client, Message message, Destination destination){
    this.client = client;
    this.message = message;
    this.destination = destination;
  }
  
  @Override
  public Response call() throws Exception {
    Response r = null;
    try {
      r = client.post(message);
      complete = true;
    } catch (RuntimeException ex){
      r = new Response();
      r.put("exception", ex.getMessage());
      ex.printStackTrace();
    } 
    return r;
  }

  public Destination getDestination() {
    return destination;
  }

  public Message getMessage() {
    return message;
  }

}
