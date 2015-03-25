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
import java.util.concurrent.atomic.AtomicReference;

import org.codehaus.jackson.map.ObjectMapper;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  //TODO. this needs to be a per column family value
  private final EventualCoordinator eventualCoordinator;
  private Hinter hinter;
  private AtomicReference<Destination> protege;
  private Tracer tracer;
  
  public Coordinator(Server server) {
    this.server = server;
    metaDataCoordinator = new MetaDataCoordinator(this, server.getConfiguration(),
            server.getMetaDataManager(), server.getClusterMembership(), server.getServerId());
    eventualCoordinator = new EventualCoordinator(server.getClusterMembership(), server.getConfiguration());
    protege = new AtomicReference<>();
  }
  
  public void init(){
    destinationLocal = new Destination();
    destinationLocal.setDestinationId(server.getServerId().getU().toString());
    metaDataCoordinator.init();
    eventualCoordinator.init();
    hinter = createHinter();
    tracer = new Tracer();
  }
  
  public static ColumnFamilyPersonality getHintCf(Server server){
    Store cf = server.getKeyspaces().get("system").getStores().get("hints");
    ColumnFamilyPersonality pers = (ColumnFamilyPersonality) cf;
    return pers;
  }
  
  private Hinter createHinter(){
    return new Hinter(getHintCf(server));  
  }
  
  public void shutdown(){
    metaDataCoordinator.shutdown();
    eventualCoordinator.shutdown();
  }

  //ah switchboad logic
  public Response handle(Message message) { 
    if (SYSTEM_KEYSPACE.equals(message.getKeyspace())) {
      return metaDataCoordinator.handleSystemMessage(message);
    }
    if (message.getPayload().containsKey("sponsor_request")){
      return handleSponsorRequest(message);
    }
    Keyspace keyspace = server.getKeyspaces().get(message.getKeyspace());
    if (keyspace == null){
      throw new RuntimeException(message.getKeyspace() + " is not found");
    }
    Store columnFamily = keyspace.getStores().get(message.getStore());
    if (columnFamily == null){
      throw new RuntimeException(message.getStore() + " is not found");
    }
    Token token = keyspace.getKeyspaceMetaData().getPartitioner().partition((String) message.getPayload().get("rowkey"));
    List<Destination> destinations = keyspace.getKeyspaceMetaData().getRouter()
            .routesTo(message, server.getServerId(), keyspace, server.getClusterMembership(), token);
    if (protege.get() != null && destinations.contains(destinationLocal) ){
      destinations.add(protege.get());
    }
    long timeoutInMs = determineTimeout(columnFamily, message);

    if (ColumnFamilyPersonality.PERSONALITY.equals(message.getPersonality())) {
      LocalAction action = new LocalColumnFamilyAction(message, keyspace, columnFamily);
      ResultMerger merger = new HighestTimestampResultMerger();
      return eventualCoordinator.handleMessage(token, message, destinations, 
              timeoutInMs, destinationLocal, action, merger, getHinterForMessage(message, columnFamily));
    } else if (KeyValuePersonality.KEY_VALUE_PERSONALITY.equals(message.getPersonality())) {
      LocalAction action = new LocalKeyValueAction(message, keyspace, columnFamily);
      ResultMerger merger = new MajorityValueResultMerger();
      return eventualCoordinator.handleMessage(token, message, destinations, 
              timeoutInMs, destinationLocal, action, merger, null);
    } else {
      throw new UnsupportedOperationException(message.getPersonality());
    }

  }
  
  private Response handleSponsorRequest(Message message){
    String requestId = (String) message.getPayload().get("request_id");
    Destination protegeDestination = new Destination();
    protegeDestination.setDestinationId(requestId);
    
    boolean res = protege.compareAndSet(null, protegeDestination);
    if (res){
      return new Response().withProperty("status", "ok");
    } else { 
      return new Response().withProperty("status", "fail").withProperty("reason", "already sponsoring") ;
    }
  }
  
  public Tracer getTracer(){
    return this.tracer;
  }
  
  private Hinter getHinterForMessage(Message message, Store columnFamily){
    String type = (String) message.getPayload().get("type");
    if (!columnFamily.getStoreMetadata().isEnableHints()){
      return null;
    }
    if (type.equals("put") || type.equals("delete") ){
      return hinter;
    } else {
      return null;
    }
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

}
