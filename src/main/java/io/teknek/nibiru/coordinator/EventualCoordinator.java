package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EventualCoordinator {

  private ExecutorService executor;
  private ConcurrentMap<Destination,ColumnFamilyClient> mapping;
  private final ClusterMembership clusterMembership;
  
  public EventualCoordinator(ClusterMembership clusterMembership){
    this.clusterMembership = clusterMembership;
  }
  
  public void init(){
    executor = Executors.newFixedThreadPool(1024);
    mapping = new ConcurrentHashMap<>();
  }
  
  private ColumnFamilyClient clientForDestination(Destination destination){
    ColumnFamilyClient client = mapping.get(destination);
    if (client != null) {
      return client;
    }
    for (ClusterMember cm : clusterMembership.getLiveMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), cm.getPort());
        mapping.putIfAbsent(destination, client);
        return cc;
      }
    }
    for (ClusterMember cm : clusterMembership.getDeadMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), cm.getPort());
        mapping.putIfAbsent(destination, client);
        return cc;
      }
    }
    throw new RuntimeException("destination does not exist");
  }

  public Response handleMessage(Token token, final Message message, List<Destination> destinations,
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
    if (!message.getPayload().containsKey("reroute")){
      message.getPayload().put("reroute", "");
    }
    Consistency c = null;
    if (consistency == null) {
      c = new Consistency().withLevel(ConsistencyLevel.N).withParameter("n", 1);
    }

    if (c.getLevel() == ConsistencyLevel.ALL) {
      List<Callable<Response>> calls = new ArrayList<Callable<Response>>();
      for (final Destination destination : destinations) {
        if (destination.equals(destinationLocal)){
          calls.add(
                  new Callable<Response>(){
                    public Response call() throws Exception {
                      return action.handleReqest();
                    }}
                  );          
        } else {
          calls.add(
                  new Callable<Response>(){
                    public Response call() throws Exception {
                      return clientForDestination(destination).post(message);
                    }    
                  }
                  );
        }
      }
      List<Future<Response>> responses = null;
      try {
         responses = executor.invokeAll(calls, 10000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
      try {
        return responses.get(0).get(1, TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        e.printStackTrace();
      }
    }

    return null;
  }
  
  public void shutdown(){
    executor.shutdown();
  }
}
