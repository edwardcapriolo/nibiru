package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.concurrent.Callable;

public class RemoteMessageCallable extends CompletableCallable implements Callable<Response> {

  private final Client client;
  private final Message message;
  
  public RemoteMessageCallable(Client client, Message message){
    this.client = client;
    this.message = message;
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
    } finally {
      if (complete == false){
        System.out.println ("making a hint for " + message);
      }
    }
    return r;
  }

}
