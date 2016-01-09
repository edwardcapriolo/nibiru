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

import java.util.List;
import io.teknek.nibiru.transport.BaseResponse;
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.Routable;
import io.teknek.nibiru.transport.columnfamily.ColumnFamilyMessage;
import io.teknek.nibiru.transport.keyvalue.KeyValueMessage;
import io.teknek.nibiru.transport.metadata.LocatorMessage;
import io.teknek.nibiru.transport.metadata.MetaDataMessage;
import io.teknek.nibiru.transport.rpc.RpcMessage;
import io.teknek.nibiru.transport.sponsor.SponsorMessage;
import io.teknek.nibiru.trigger.TriggerManager;
import io.teknek.nibiru.transport.columnfamily.*;
import io.teknek.nibiru.transport.columnfamilyadmin.ColumnFamilyAdminMessage;
import io.teknek.nibiru.transport.directsstable.DirectSsTableMessage;

public class Coordinator {

  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  private final EventualCoordinator eventualCoordinator;
  private final SponsorCoordinator sponsorCoordinator;
  private final RpcCoordinator rpcCoordinator;
  private final ColumnFamilyAdminCoordinator columnFamilyAdminCoordinator;
  private final DirectSsTableCoordinator directSsTableCoordinator;
  private final Locator locator;
  private final TriggerManager triggerManager;
  private Hinter hinter;
  private Tracer tracer;
  
  public Coordinator(Server server) {
    this.server = server;
    metaDataCoordinator = new MetaDataCoordinator(this, server.getConfiguration(),
            server.getMetaDataManager(), server.getClusterMembership(), server.getServerId());
    eventualCoordinator = new EventualCoordinator(server.getClusterMembership(), server.getConfiguration());
    sponsorCoordinator = new SponsorCoordinator(server.getClusterMembership(), server.getMetaDataManager(), metaDataCoordinator, server);
    locator = new Locator(server.getConfiguration(), server.getClusterMembership());
    rpcCoordinator = new RpcCoordinator(server.getConfiguration());
    triggerManager = new TriggerManager(server);
    columnFamilyAdminCoordinator = new ColumnFamilyAdminCoordinator(server);
    directSsTableCoordinator = new DirectSsTableCoordinator(server);
  }
  
  public void init(){
    destinationLocal = new Destination();
    destinationLocal.setDestinationId(server.getServerId().getU().toString());
    metaDataCoordinator.init();
    eventualCoordinator.init();
    hinter = createHinter();
    tracer = new Tracer();
    triggerManager.init();
    rpcCoordinator.init();
  }
  
  public static ColumnFamilyPersonality getHintCf(Server server){
    Store cf = server.getKeyspaces().get(MetaDataManager.SYSTEM_KEYSPACE).getStores().get("hints");
    ColumnFamilyPersonality pers = (ColumnFamilyPersonality) cf;
    return pers;
  }
  
  private Hinter createHinter(){
    return new Hinter(getHintCf(server));  
  }
  
  public void shutdown(){
    metaDataCoordinator.shutdown();
    eventualCoordinator.shutdown();
    triggerManager.shutdown();
    rpcCoordinator.shutdown();
  }

  public List<Destination> destinationsForToken(Token token, Keyspace keyspace){
    return keyspace.getKeyspaceMetaData().getRouter()
            .routesTo(server.getServerId(), keyspace, server.getClusterMembership(), token);
  }
  
  public BaseResponse handle(BaseMessage baseMessage) {
    if (baseMessage instanceof RpcMessage){
      return rpcCoordinator.processMessage((RpcMessage) baseMessage);
    } else if (baseMessage instanceof ColumnFamilyAdminMessage){
      return columnFamilyAdminCoordinator.handleMessage((ColumnFamilyAdminMessage) baseMessage);
    } else if (baseMessage instanceof DirectSsTableMessage){
      return directSsTableCoordinator.handleStreamRequest((DirectSsTableMessage) baseMessage);
    } else if (baseMessage instanceof SponsorMessage){
      return sponsorCoordinator.handleSponsorRequest((SponsorMessage) baseMessage);
    } else if (baseMessage instanceof MetaDataMessage ) {
      return metaDataCoordinator.handleSystemMessage((Message) baseMessage);
    }
    Keyspace keyspace = null;
    Store store = null;
    if (baseMessage instanceof ColumnFamilyMessage){
      ColumnFamilyMessage m = (ColumnFamilyMessage) baseMessage;
      keyspace = server.getKeyspaces().get(m.getKeyspace());
      store = keyspace.getStores().get(m.getStore());
    }
    if (baseMessage instanceof KeyValueMessage){
      KeyValueMessage m = (KeyValueMessage) baseMessage;
      keyspace = server.getKeyspaces().get(m.getKeyspace());
      store = keyspace.getStores().get(m.getStore());
    }
    if (baseMessage instanceof LocatorMessage){
      LocatorMessage m = (LocatorMessage) baseMessage;
      keyspace = server.getKeyspaces().get(m.getKeyspace());
    }
    if (keyspace == null){
      throw new RuntimeException("keyspace is not found" + baseMessage);
    }
    Token token = null;
    if (baseMessage instanceof Routable){
      Routable r = (Routable) baseMessage;
      token = keyspace.getKeyspaceMetaData().getPartitioner().partition(r.determineRoutingInformation());
    } 
    List<Destination> destinations = destinationsForToken(token, keyspace);
    if (baseMessage instanceof LocatorMessage){
      return locator.locate(destinations);
    }
    if (store == null){
      throw new RuntimeException("store is not found" + baseMessage);
    }
    
    /*
    if (sponsorCoordinator.getProtege() != null && destinations.contains(destinationLocal)){
      //TODO they only need some of the messages by range
      String type = (String) m.getPayload().get("type");
      if (type.equals("put") || type.equals("delete") ){ 
        destinations.add(sponsorCoordinator.getProtege());
      }
    }
    
    long timeoutInMs = determineTimeout(store, m);
    long requestStart = System.currentTimeMillis();
    */
    long timeoutInMs = 10000;
    long requestStart = System.currentTimeMillis();

    if (baseMessage instanceof ColumnFamilyMessage) {
      ColumnFamilyMessage m = (ColumnFamilyMessage) baseMessage;
      LocalAction action = new LocalColumnFamilyAction(m, keyspace, store);
      ResultMerger merger = new HighestTimestampResultMerger();
      Response response = eventualCoordinator.handleMessage(token, m, destinations, 
              timeoutInMs, destinationLocal, action, merger, getHinterForMessage(baseMessage, store));
      if (!response.containsKey("exception")){
        response = triggerManager.executeTriggers(m, response, keyspace, store, timeoutInMs, requestStart);
      }
      return response;
    } else if ( baseMessage instanceof KeyValueMessage) {
      LocalAction action = new LocalKeyValueAction(baseMessage, keyspace, store);
      ResultMerger merger = new MajorityValueResultMerger();
      Response response = eventualCoordinator.handleMessage(token, baseMessage, destinations, 
              timeoutInMs, destinationLocal, action, merger, null);
      triggerManager.executeTriggers(baseMessage, response, keyspace, store, timeoutInMs, requestStart);
      if (!response.containsKey("exception")){
        response = triggerManager.executeTriggers(baseMessage, response, keyspace, store, timeoutInMs, requestStart);
      }
      return response;
    } else {
      throw new UnsupportedOperationException(baseMessage.toString());
    }

  }
  
  public Tracer getTracer(){
    return this.tracer;
  }
  
  private Hinter getHinterForMessage(BaseMessage message, Store columnFamily){
    if (!columnFamily.getStoreMetadata().isEnableHints()){
      return null;
    }
    if (message instanceof GetMessage){
      return null;
    }
    if (message instanceof Message){
      Message m = (Message) message;
      String type = (String) m.getPayload().get("type");
      if (type.equals("put") || type.equals("delete") ){
        return hinter;
      } else {
        return null;
      }
    }
    return null;
  }
  
  public Hinter getHinter() {
    return hinter;
  }

  private static long determineTimeout(Store columnFamily, Message message){
    if (message.getPayload().containsKey("timeout")){
      return ((Number) message.getPayload().get("timeout")).longValue();
    } else {
      return columnFamily.getStoreMetadata().getOperationTimeoutInMs();
    }
  }

  public SponsorCoordinator getSponsorCoordinator() {
    return sponsorCoordinator;
  }

  public Destination getDestinationLocal() {
    return destinationLocal;
  }

}
