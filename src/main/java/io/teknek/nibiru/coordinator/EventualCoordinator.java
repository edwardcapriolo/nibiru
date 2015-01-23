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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.security.auth.login.Configuration;

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
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), 7070);
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    for (ClusterMember cm : clusterMembership.getDeadMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), 7070);
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
      ExecutorCompletionService<Response> ec = new ExecutorCompletionService<>(executor);  
      final ArrayBlockingQueue<Response> results = new ArrayBlockingQueue<Response>(100);
      ArrayBlockingQueue<Future<Response>> futures = new ArrayBlockingQueue<Future<Response>>(100);
      for (final Destination destination : destinations) {
        Future<Response> f = null;
        if (destination.equals(destinationLocal)){
          f = ec.submit(new LocalActionCallable(results, action));
        } else {
          f = ec.submit(new RemoteMessageCallable(results, clientForDestination(destination), message));
        }
        futures.add(f);
      }
      long start = System.currentTimeMillis();
      long deadline = start + (1L * 1000L) ;
      List<Response> responses = new ArrayList<>();
      while (start <= deadline){
        Response r = null;
        try {
          r = ec.poll(deadline - start, TimeUnit.MILLISECONDS).get();
          responses.add(r);
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
        }
        if (r == null){
          r = new Response();
          r.put("exception", "coordinator timeout");
          return r;
        }
        if (r.containsKey("exception")){
          return r;
        }
        if (responses.size() == destinations.size()){
          return new Response();
        }
        start = System.currentTimeMillis();
      }
          
    }
    
    return null;
  }
  
  public void shutdown(){
    executor.shutdown();
  }
}
