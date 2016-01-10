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
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.metadata.*;

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
  
  public MetaDataClient clientForClusterMember(ClusterMember clusterMember){
    MetaDataClient c = clients.get(clusterMember);
    if (c == null) {
      c = new MetaDataClient(clusterMember.getHost(), configuration
              .getTransportPort());
      clients.putIfAbsent(clusterMember, c);
    }
    return c;
  }
    
  public Response handleSystemMessage(final MetaDataMessage message){
    if (message instanceof ListLiveMembers){
      return handleListLiveMembersMessage((ListLiveMembers) message);
    } else if (message instanceof CreateOrUpdateKeyspace) { 
      return handleCreateOrUpdateKeyspace((CreateOrUpdateKeyspace) message);
    } else if (message instanceof ListKeyspaces){
      return handleListKeyspaces((ListKeyspaces) message);
    } else if (message instanceof CreateOrUpdateStore){
      return handleCreateOrUpdateStore((CreateOrUpdateStore) message);
    } else if (message instanceof ListStores){
      return handleListStores((ListStores) message);
    } else if (message instanceof GetKeyspaceMetaData){ 
      return handleGetKeyspaceMetaData((GetKeyspaceMetaData) message);
    } else if (message instanceof GetStoreMetaData){ 
      return handleGetStoreMetaData((GetStoreMetaData) message);
    } else {
      throw new IllegalArgumentException(this.getClass().getName() + " could not process " + message);
    }
  }
  
  private Response handleListStores(ListStores message) {
    String keyspace = message.getKeyspace();
    return new Response().withProperty("payload", metaDataManager.listStores(keyspace));
  }

  private Response handleGetKeyspaceMetaData(GetKeyspaceMetaData message) {
    Response r = new Response().withProperty("payload", 
            metaDataManager.getKeyspaceMetadata(message.getKeyspace()).getProperties());
    return r;
  }
  
  private Response handleGetStoreMetaData(GetStoreMetaData message) {
    Response r = new Response().withProperty("payload", 
            metaDataManager.getStoreMetadata(message.getKeyspace(), message.getStore()).getProperties());
    return r;
  }
  
  
  private Response handleCreateOrUpdateStore(final CreateOrUpdateStore message){
    metaDataManager.createOrUpdateStore(message.getKeyspace(), message.getStore(), message.getProperties());
    if (message.isShouldReroute()){
      message.setShouldReroute(false);
      List<Callable<Void>> calls = new ArrayList<>();
      for (ClusterMember clusterMember : clusterMembership.getLiveMembers()){
        final MetaDataClient c = clientForClusterMember(clusterMember);
        Callable<Void> call = new Callable<Void>(){
          public Void call() throws Exception {
            c.createOrUpdateStore(
                    message.getKeyspace(), message.getStore(), message.getProperties(), false);
            return null;
          }};
        calls.add(call); 
      }
      try {
        List<Future<Void>> res = metaExecutor.invokeAll(calls, 10, TimeUnit.SECONDS);
        //todo return results to client
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      
    }
    return new Response();  
  }
  
  private Response handleListKeyspaces(final ListKeyspaces message){
    return new Response().withProperty("payload", metaDataManager.listKeyspaces());
  }
  
  private Response handleCreateOrUpdateKeyspace(final CreateOrUpdateKeyspace message){
    
    metaDataManager.createOrUpdateKeyspace(
            (String) message.getKeyspace(), 
            (Map<String,Object>) message.getProperties());
    //TODO this is hokey
    if (message.isShouldReRoute()){
      message.setShouldReRoute(false);
      List<Callable<Void>> calls = new ArrayList<>();
      for (ClusterMember clusterMember : clusterMembership.getLiveMembers()){
        final MetaDataClient c = clientForClusterMember(clusterMember);
        Callable<Void> call = new Callable<Void>(){
          public Void call() throws Exception {
            try {
              c.createOrUpdateKeyspace(
                    (String) message.getKeyspace(), 
                    (Map<String,Object>) message.getProperties(), false
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
      } catch (InterruptedException e) {
        System.err.println(e);
      }
    }
    
    return new Response();
  }
  
  private Response handleListLiveMembersMessage(final ListLiveMembers message){
    List<ClusterMember> copy = new ArrayList<>();
    copy.addAll(clusterMembership.getLiveMembers());
    ClusterMember me = new ClusterMember();
    me.setHeatbeat(0);
    me.setHost(configuration.getTransportHost());
    me.setPort(1);//TODO 
    me.setId(serverId.getU().toString());
    copy.add(me);
    return new Response().withProperty("payload", copy);
  }
  
}
