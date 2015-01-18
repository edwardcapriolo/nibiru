package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EventualCoordinator {

  private ExecutorService executor;
  
  public EventualCoordinator(){
    
  }
  
  public Response handleMessage(Token token, Message message, List<Destination> destinations, long timeoutInMs, Consistency consistency){
    Consistency c = null;
    if (consistency == null){
      c = new Consistency().withLevel(ConsistencyLevel.N).withParameter("n", 1);
    }
    if (c.getLevel() == ConsistencyLevel.ALL){
      List<Callable<Response>> calls = new ArrayList<Callable<Response>>();
      for (Destination destination: destinations){
        //id destination is
      }
    }
    
    return null;
  }
  
  public void init(){
    executor = Executors.newFixedThreadPool(1024);
  }
  
  public void destroy(){
    executor.shutdown();
  }
}
