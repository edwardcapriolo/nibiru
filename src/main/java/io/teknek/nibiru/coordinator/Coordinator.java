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
import org.codehaus.jackson.map.jsontype.NamedType;
import org.codehaus.jackson.type.TypeReference;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.InternodeClient.AtomPair;
import io.teknek.nibiru.engine.DirectSsTableWriter;
import io.teknek.nibiru.engine.SsTableStreamWriter;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.KeyValuePersonality;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.metadata.MetaDataMessage;

public class Coordinator {

  private static final String SYSTEM_KEYSPACE = "system";  
  private final Server server;
  private Destination destinationLocal;
  private final MetaDataCoordinator metaDataCoordinator;
  //TODO. this needs to be a per column family value
  private final EventualCoordinator eventualCoordinator;
  private final SponsorCoordinator sponsorCoordinator;
  private final Locator locator;
  
  private Hinter hinter;
  private Tracer tracer;
  
  
  public Coordinator(Server server) {
    this.server = server;
    metaDataCoordinator = new MetaDataCoordinator(this, server.getConfiguration(),
            server.getMetaDataManager(), server.getClusterMembership(), server.getServerId());
    eventualCoordinator = new EventualCoordinator(server.getClusterMembership(), server.getConfiguration());
    sponsorCoordinator = new SponsorCoordinator(server.getClusterMembership(), server.getMetaDataManager(), metaDataCoordinator, server);
    locator = new Locator(server.getConfiguration(), server.getClusterMembership());
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

  public Response handleStreamRequest(Message m){
    
    ObjectMapper om = new ObjectMapper();

    
    Store store = this.server.getKeyspaces().get(m.getPayload().get("keyspace"))
            .getStores().get(m.getPayload().get("store"));
    DirectSsTableWriter w = (DirectSsTableWriter) store;
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
    return null;
   
  }
  
  //ah switchboad logic
  public Response handle(Message message) { 
    if (DirectSsTableWriter.PERSONALITY.equals(message.getPersonality())){
      return handleStreamRequest(message);
    }
    if (message.getPayload().containsKey("sponsor_request")){
      return sponsorCoordinator.handleSponsorRequest(message);
    }
    
    if (SYSTEM_KEYSPACE.equals(message.getKeyspace()) || message instanceof MetaDataMessage ) {
      return metaDataCoordinator.handleSystemMessage(message);
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
    if (LocatorPersonality.PERSONALITY.equals(message.getPersonality())){
      return locator.locate(destinations);
    }
    
    
    if (sponsorCoordinator.getProtege() != null && destinations.contains(destinationLocal)){
      //TODO they only need some of the messages by range
      String type = (String) message.getPayload().get("type");
      if (type.equals("put") || type.equals("delete") ){ 
        destinations.add(sponsorCoordinator.getProtege());
      }
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

  public SponsorCoordinator getSponsorCoordinator() {
    return sponsorCoordinator;
  }

}
