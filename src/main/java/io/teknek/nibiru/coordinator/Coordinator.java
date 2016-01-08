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
import java.util.SortedMap;
import java.util.TreeMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import io.teknek.nibiru.transport.BaseResponse;
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.InternodeClient.AtomPair;
import io.teknek.nibiru.engine.DirectSsTableWriter;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.personality.ColumnFamilyAdminPersonality;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.Routable;
import io.teknek.nibiru.transport.columnfamily.ColumnFamilyMessage;
import io.teknek.nibiru.transport.keyvalue.KeyValueMessage;
import io.teknek.nibiru.transport.metadata.MetaDataMessage;
import io.teknek.nibiru.transport.rpc.RpcMessage;
import io.teknek.nibiru.trigger.TriggerManager;
import io.teknek.nibiru.transport.columnfamily.*;
import io.teknek.nibiru.transport.directsstable.*;
import io.teknek.nibiru.transport.columnfamilyadmin.ColumnFamilyAdminMessage;
import io.teknek.nibiru.transport.directsstable.DirectSsTableMessage;
public class Coordinator {

  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  //TODO. this needs to be a per column family value
  private final EventualCoordinator eventualCoordinator;
  private final SponsorCoordinator sponsorCoordinator;
  private final RpcCoordinator rpcCoordinator;
  private final ColumnFamilyAdminCoordinator columnFamilyAdminCoordinator;
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

  public Response handleStreamRequest(DirectSsTableMessage m){
    Store store = server.getKeyspaces().get(m.getKeyspace())
            .getStores().get(m.getStore());
    DirectSsTableWriter w = (DirectSsTableWriter) store;
    if (m instanceof Open){
      w.open(m.getId());
      return new Response();
    } else if (m instanceof Close){
      w.close(m.getId());
    } else if (m instanceof Write){
      Write write = (Write) m;
      SortedMap<AtomKey, AtomValue> mp = new TreeMap<>();
      for (AtomPair aPair: write.getColumns()){
        mp.put(aPair.getKey(), aPair.getValue());
      }
      w.write(write.getToken(), mp, write.getId());
    } else {
      throw new RuntimeException("hit rock bottom");
    }
    /*
    if (DirectSsTableWriter.OPEN.equals(m.getPayload().get("type"))){
      w.open((String) m.getPayload().get("id"));
      return new Response();
    } else if (DirectSsTableWriter.CLOSE.equals(m.getPayload().get("type"))){
      w.close((String) m.getPayload().get("id"));
      return new Response();
    } else if (DirectSsTableWriter.WRITE.equals(m.getPayload().get("type"))){
      TypeReference<List<AtomPair>> t = new TypeReference<List<AtomPair>>() { };
      List<AtomPair> pair = om.convertValue( m.getPayload().get("columns"), t);
      SortedMap<AtomKey, AtomValue> mp = new TreeMap<>();
      for (AtomPair aPair: pair){
        mp.put(aPair.getKey(), aPair.getValue());
      }
      w.write(om.convertValue( m.getPayload().get("token"), Token.class), mp,
              (String)m.getPayload().get("id"));
      return new Response();
    }
    */
    return null;
   
  }
  
  public List<Destination> destinationsForToken(Token token, Keyspace keyspace){
    return keyspace.getKeyspaceMetaData().getRouter()
            .routesTo(server.getServerId(), keyspace, server.getClusterMembership(), token);
  }
  
  public BaseResponse handle(BaseMessage baseMessage) {
    if (baseMessage instanceof RpcMessage){
      return rpcCoordinator.processMessage((RpcMessage) baseMessage);
    }
    if (baseMessage instanceof ColumnFamilyAdminMessage){
      return this.columnFamilyAdminCoordinator.handleMessage((ColumnFamilyAdminMessage) baseMessage);
    }
    if (baseMessage instanceof DirectSsTableMessage){
      return this.handleStreamRequest((DirectSsTableMessage) baseMessage);
    }
    Message m = null;
    if (baseMessage instanceof Message){
      m = (Message) baseMessage;
      
      if (m.getPayload().containsKey("sponsor_request")){
        return sponsorCoordinator.handleSponsorRequest(m);
      }
      if (baseMessage instanceof MetaDataMessage ) {
        return metaDataCoordinator.handleSystemMessage(m);
      }
      Keyspace keyspace = server.getKeyspaces().get(m.getKeyspace());
      if (keyspace == null){
        throw new RuntimeException(m.getKeyspace() + " is not found");
      }
      Store columnFamily = keyspace.getStores().get(m.getStore());
      if (columnFamily == null){
        throw new RuntimeException(m.getStore() + " is not found");
      }
      
      Token token = null;
      if (baseMessage instanceof Routable){
        Routable r = (Routable) baseMessage;
        token = keyspace.getKeyspaceMetaData().getPartitioner().partition(r.determineRoutingInformation());
      } else {
        //TODO remove after conversion
        token = keyspace.getKeyspaceMetaData().getPartitioner().partition((String) m.getPayload().get("rowkey"));
      }
      List<Destination> destinations = destinationsForToken(token, keyspace);
      if (LocatorPersonality.PERSONALITY.equals(m.getPersonality())){
        return locator.locate(destinations);
      }
      
      
      if (sponsorCoordinator.getProtege() != null && destinations.contains(destinationLocal)){
        //TODO they only need some of the messages by range
        String type = (String) m.getPayload().get("type");
        if (type.equals("put") || type.equals("delete") ){ 
          destinations.add(sponsorCoordinator.getProtege());
        }
      }
      long timeoutInMs = determineTimeout(columnFamily, m);
      long requestStart = System.currentTimeMillis();
  
      if (ColumnFamilyPersonality.PERSONALITY.equals(m.getPersonality()) || baseMessage instanceof ColumnFamilyMessage) {
        LocalAction action = new LocalColumnFamilyAction(m, keyspace, columnFamily);
        ResultMerger merger = new HighestTimestampResultMerger();
        Response response = eventualCoordinator.handleMessage(token, m, destinations, 
                timeoutInMs, destinationLocal, action, merger, getHinterForMessage(baseMessage, columnFamily));
        if (!response.containsKey("exception")){
          response = triggerManager.executeTriggers(m, response, keyspace, columnFamily, timeoutInMs, requestStart);
        }
        return response;
      } else if ( baseMessage instanceof KeyValueMessage) {
        LocalAction action = new LocalKeyValueAction(m, keyspace, columnFamily);
        ResultMerger merger = new MajorityValueResultMerger();
        Response response = eventualCoordinator.handleMessage(token, m, destinations, 
                timeoutInMs, destinationLocal, action, merger, null);
        triggerManager.executeTriggers(m, response, keyspace, columnFamily, timeoutInMs, requestStart);
        if (!response.containsKey("exception")){
          response = triggerManager.executeTriggers(m, response, keyspace, columnFamily, timeoutInMs, requestStart);
        }
        return response;
      } else {
        throw new UnsupportedOperationException(m.getPersonality());
      }
    }
    return null;
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
