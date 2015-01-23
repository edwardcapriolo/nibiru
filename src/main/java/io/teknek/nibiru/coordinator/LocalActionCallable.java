package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.transport.Response;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class LocalActionCallable implements Callable<Response>{

  private final ArrayBlockingQueue<Response> results;
  private final LocalAction action;
  
  public LocalActionCallable(final ArrayBlockingQueue<Response> results, final LocalAction action){
    this.results = results;
    this.action = action;
  }
  
  @Override
  public Response call() throws Exception {
    Response r = null;
    try {
      r = action.handleReqest();
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
