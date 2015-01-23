package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class RemoteMessageCallable implements Callable<Response> {

  private final ArrayBlockingQueue<Response> results; 
  private final Client client;
  private final Message message;
  
  public RemoteMessageCallable(ArrayBlockingQueue<Response> results, Client client, Message message ){
    this.results = results;
    this.client = client;
    this.message = message;
  }
  
  @Override
  public Response call() throws Exception {
    Response r = null;
    try {
      r = client.post(message);
      results.add(r);
    } catch (RuntimeException ex){
      r = new Response();
      r.put("exception", ex.getMessage());
      ex.printStackTrace();
      results.add(r);
    }
    return r;
  }

}
