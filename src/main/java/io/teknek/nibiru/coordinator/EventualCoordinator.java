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

  public Response handleMessage(Token token, Message message, List<Destination> destinations,
          long timeoutInMs, Consistency consistency, Destination destinationLocal, final LocalAction action) {
    if (destinations.size() == 0 ){
      throw new RuntimeException("No place to route message");
    }
    if (destinations.size() == 1 && destinations.contains(destinationLocal)) {
      return action.handleReqest();
    }
    //TODO if the message is routed here but here is not the correct place
    // we should route it again
    if (message.getPayload().containsKey("reroute")){
      return action.handleReqest();
    } 
    Consistency c = null;
    if (consistency == null) {
      c = new Consistency().withLevel(ConsistencyLevel.N).withParameter("n", 1);
    }

    if (c.getLevel() == ConsistencyLevel.ALL) {
      List<Callable<Response>> calls = new ArrayList<Callable<Response>>();
      for (Destination destination : destinations) {
        if (destination.equals(destinationLocal)){
          calls.add(
                  new Callable<Response>(){
                    public Response call() throws Exception {
                      return action.handleReqest();
                    }}
                  );          
        } else {
          
        }
      }
    }

    return null;
  }

  public void init(){
    executor = Executors.newFixedThreadPool(1024);
  }
  
  public void shutdown(){
    executor.shutdown();
  }
}
