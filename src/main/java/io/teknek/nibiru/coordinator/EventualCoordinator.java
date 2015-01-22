/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import org.codehaus.jackson.map.ObjectMapper;

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
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    for (ClusterMember cm : clusterMembership.getDeadMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), cm.getPort());
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    throw new RuntimeException(String.format(
            "destination %s does not exist. Live members %s. Dead members %s", destination.getDestinationId(), 
            clusterMembership.getLiveMembers(), clusterMembership.getDeadMembers()));
  }

  public Response handleMessage(Token token, final Message message, List<Destination> destinations,
          long timeoutInMs, Destination destinationLocal, final LocalAction action) {
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
    if (message.getPayload().get("consistency") == null) {
      message.getPayload().put("consistency",
              new Consistency().withLevel(ConsistencyLevel.N).withParameter("n", 1));
    }
    ObjectMapper om = new ObjectMapper();
    c = om.convertValue( message.getPayload().get("consistency"), Consistency.class);

    if (c.getLevel() == ConsistencyLevel.ALL) {
      List<Callable<Response>> calls = new ArrayList<Callable<Response>>();
      for (final Destination destination : destinations) {
        if (destination.equals(destinationLocal)){
          calls.add(
                  new Callable<Response>(){
                    public Response call() throws Exception {
                      try {
                      return action.handleReqest();
                      } catch (RuntimeException ex){
                        ex.printStackTrace();
                      }
                      return null;
                    }}
                  );          
        } else {
          calls.add(
                  new Callable<Response>(){
                    public Response call() throws Exception {
                      try {
                        return clientForDestination(destination).post(message);
                      } catch (RuntimeException ex){
                        ex.printStackTrace();
                      }
                      return null;
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
