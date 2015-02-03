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

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;

public class EventualCoordinator {

  private ExecutorService executor;
  private final Configuration configuration;
  private ConcurrentMap<Destination,ColumnFamilyClient> mapping;
  private final ClusterMembership clusterMembership;
  private static final ObjectMapper OM = new ObjectMapper();
  
  
  public EventualCoordinator(ClusterMembership clusterMembership, Configuration configuration){
    this.clusterMembership = clusterMembership;
    this.configuration = configuration;
  }
  
  public void init(){
    executor = Executors.newFixedThreadPool(1024);
    mapping = new ConcurrentHashMap<>();
  }
  
  private Client clientForDestination(Destination destination){
    Client client = mapping.get(destination);
    if (client != null) {
      return client;
    }
    for (ClusterMember cm : clusterMembership.getLiveMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), configuration.getTransportPort());
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    for (ClusterMember cm : clusterMembership.getDeadMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), configuration.getTransportPort());
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    throw new RuntimeException(String.format(
            "destination %s does not exist. Live members %s. Dead members %s", destination.getDestinationId(), 
            clusterMembership.getLiveMembers(), clusterMembership.getDeadMembers()));
  }

  public Response handleMessage(Token token, final Message message, List<Destination> destinations,
          long timeoutInMs, Destination destinationLocal, final LocalAction action, ResultMerger merger) {
    if (destinations.size() == 0){
      throw new RuntimeException("No place to route message");
    }
    if (destinations.size() == 1 && destinations.contains(destinationLocal)) {
      return action.handleReqest();
    }
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
    } else {
      c = OM.convertValue( message.getPayload().get("consistency"), Consistency.class);
    }
    
    ExecutorCompletionService<Response> completionService = new ExecutorCompletionService<>(executor);
    List<RemoteMessageCallable> remote = new ArrayList<>();
    for (final Destination destination : destinations) {
      if (destination.equals(destinationLocal)) {
        completionService.submit(new LocalActionCallable(action));
      } else {
        RemoteMessageCallable r = new RemoteMessageCallable(clientForDestination(destination), message);
        remote.add(r);
        completionService.submit(r);
      }
    }
    long start = System.currentTimeMillis();
    long deadline = start + timeoutInMs;
    if (c.getLevel() == ConsistencyLevel.ALL) {
      return handleAll(start, deadline, completionService, destinations, merger, message);
    } else if (c.getLevel() == ConsistencyLevel.N) {
      return this.handleN(start, deadline, completionService, destinations, merger, message, c);
    } else {
      return new Response().withProperty("exception", "unsupported consistency level");
    }
  }
  
  private Response handleN(long start, long deadline,
          ExecutorCompletionService<Response> completionService, List<Destination> destinations,
          ResultMerger merger, Message message, Consistency c) {
    List<Response> responses = new ArrayList<>();
    int wantedResults = (Integer) c.getParameters().get("n");
    int sucessfulSoFar = 0;
    while (start <= deadline) {
      Response r = null;
      try {
        Future<Response> future = completionService.poll(deadline - start, TimeUnit.MILLISECONDS);
        r = future.get();
        if (r != null){
          responses.add(r);
          sucessfulSoFar++;
        }
        if (sucessfulSoFar >= wantedResults) {
          break;
        }
      } catch (InterruptedException | ExecutionException e) {
        continue;
      }
      start = System.currentTimeMillis();
    } 
    return merger.merge(responses, message);
  }

  private Response handleAll(long start, long deadline,
          ExecutorCompletionService<Response> completionService, List<Destination> destinations,
          ResultMerger merger, Message message) {
    List<Response> responses = new ArrayList<>();
    while (start <= deadline) {
      Response r = null;
      try {
        Future<Response> future = completionService.poll(deadline - start, TimeUnit.MILLISECONDS);
        r = future.get();
        if (r != null) {
          responses.add(r);
        }
      } catch (InterruptedException | ExecutionException e) {
        return new Response().withProperty("exception", "coordinator timeout");
      }
      if (r == null) {
        return new Response().withProperty("exception", "coordinator timeout");
      }
      if (r.containsKey("exception")) {
        return r;
      }
      if (responses.size() == destinations.size()) {
        break;
      }
      start = System.currentTimeMillis();
    }
    if (responses.size() == destinations.size()) {
      return merger.merge(responses, message);
    } else {
      return new Response().withProperty("exception", "coordinator timeout");
    }
  }
  
  public void shutdown(){
    executor.shutdown();
  }
}
