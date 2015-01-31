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
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.personality.MetaPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**evenually this will have to be plugable per keyspace. For now code serparation */
public class MetaDataCoordinator {

  @SuppressWarnings("unused")
  private final Coordinator coordinator;
  private final Configuration configuration;
  private final MetaDataManager metaDataManager;
  private final ClusterMembership clusterMembership;
  private final ServerId serverId;
  private ExecutorService metaExecutor;
  private ConcurrentMap<ClusterMember,MetaDataClient> clients;
  
  public MetaDataCoordinator(Coordinator c, Configuration configuration,
          MetaDataManager metaDataManager, ClusterMembership clusterMembership, ServerId serverId) {
    this.coordinator = c;
    this.configuration = configuration;
    this.metaDataManager = metaDataManager;
    this.clusterMembership = clusterMembership;
    this.serverId = serverId;
  }
  
  public void init(){
    clients = new ConcurrentHashMap<>();
    metaExecutor = Executors.newFixedThreadPool(1024);
  }
  
  public void shutdown(){
    metaExecutor.shutdown();
    try {
      metaExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
  }
  
  private MetaDataClient clientForClusterMember(ClusterMember clusterMember){
    MetaDataClient c = clients.get(clusterMember);
    if (c == null) {
      c = new MetaDataClient(clusterMember.getHost(), configuration
              .getTransportPort());
      clients.putIfAbsent(clusterMember, c);
    }
    return c;
  }
  
  @SuppressWarnings("unchecked")
  public Response handleSystemMessage(final Message message){
    if (MetaPersonality.LIST_LIVE_MEMBERS.equals(message.getPayload().get("type"))){
      List<ClusterMember> copy = new ArrayList<>();
      copy.addAll(clusterMembership.getLiveMembers());
      ClusterMember me = new ClusterMember();
      me.setHeatbeat(0);
      me.setHost(configuration.getTransportHost());
      me.setPort(1);//TODO 
      me.setId(serverId.getU().toString());
      copy.add(me);
      return new Response().withProperty("payload", copy);
    } else if (MetaPersonality.CREATE_OR_UPDATE_KEYSPACE.equals(message.getPayload().get("type"))){
      metaDataManager.createOrUpdateKeyspace(
              (String) message.getPayload().get("keyspace"), 
              (Map<String,Object>) message.getPayload());
      if (!message.getPayload().containsKey("reroute")){
        message.getPayload().put("reroute", "");
        List<Callable<Void>> calls = new ArrayList<>();
        for (ClusterMember clusterMember : clusterMembership.getLiveMembers()){
          final MetaDataClient c = clientForClusterMember(clusterMember);
          Callable<Void> call = new Callable<Void>(){
            public Void call() throws Exception {
              try {
              c.createOrUpdateKeyspace(
                      (String) message.getPayload().get("keyspace"), 
                      //(Map<String,Object>) message.getPayload().get("properties")
                      (Map<String,Object>) message.getPayload()
                      );
             
              } catch (RuntimeException ex){
                ex.printStackTrace();
              }
              return null;
            }};
          calls.add(call); 
        }
        try {
          List<Future<Void>> res = metaExecutor.invokeAll(calls, 10, TimeUnit.SECONDS);
          System.out.println(res);
        } catch (InterruptedException e) {

        }
      }
      return new Response();
    } else if (MetaPersonality.CREATE_OR_UPDATE_COLUMN_FAMILY.equals(message.getPayload().get("type"))){
      metaDataManager.createOrUpdateColumnFamily((String) message.getPayload().get("keyspace"),
              (String) message.getPayload().get("columnfamily"),
              (Map<String,Object>) message.getPayload());
      if (!message.getPayload().containsKey("reroute")){
        message.getPayload().put("reroute", "");
        List<Callable<Void>> calls = new ArrayList<>();
        for (ClusterMember clusterMember : clusterMembership.getLiveMembers()){
          final MetaDataClient c = clientForClusterMember(clusterMember);
          Callable<Void> call = new Callable<Void>(){
            public Void call() throws Exception {
              c.createOrUpdateColumnFamily(
                      (String) message.getPayload().get("keyspace"),
                      (String) message.getPayload().get("columnfamily"),
                      (Map<String,Object>) message.getPayload());
              return null;
            }};
          calls.add(call); 
        }
        try {
          List<Future<Void>> res = metaExecutor.invokeAll(calls, 10, TimeUnit.SECONDS);
          //todo return results to client
        } catch (InterruptedException e) {

        }
      }
      return new Response();
    } else {
      throw new IllegalArgumentException("could not process " + message);
    }
    
  }
}
