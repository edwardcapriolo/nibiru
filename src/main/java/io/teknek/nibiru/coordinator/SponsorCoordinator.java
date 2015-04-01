package io.teknek.nibiru.coordinator;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.InternodeClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.SsTable;
import io.teknek.nibiru.engine.SsTableStreamReader;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class SponsorCoordinator {

  private final ClusterMembership clusterMembership;
  private final MetaDataManager metaDataManager;
  private final MetaDataCoordinator metaDataCoordinator;
  private final AtomicReference<Destination> protege;
  private final AtomicReference<String> protogeToken;
  private final Server server;
  
  public SponsorCoordinator(ClusterMembership clusterMembership,
          MetaDataManager metaDataManager, MetaDataCoordinator metaDataCoordinator, Server server) {
    this.clusterMembership = clusterMembership;
    this.metaDataManager = metaDataManager;
    this.metaDataCoordinator = metaDataCoordinator;
    this.protege = new AtomicReference<>();
    this.protogeToken = new AtomicReference<>();
    this.server = server;
  }
  
  public Response handleSponsorRequest(final Message message){
    String requestId = (String) message.getPayload().get("request_id");
    final String joinKeyspace = (String) message.getPayload().get("keyspace");
    String wantedToken = (String) message.getPayload().get("wanted_token");
    final String protogeHost = (String) message.getPayload().get("transport_host");
    Destination protegeDestination = new Destination();
    protegeDestination.setDestinationId(requestId);
    MetaDataClient metaDataClient = null;
    for (ClusterMember cm : clusterMembership.getLiveMembers()){
      if (cm.getId().equals(protegeDestination.getDestinationId())){
        metaDataClient = metaDataCoordinator.clientForClusterMember(cm);
      }
    }
    if (metaDataClient == null){
      throw new RuntimeException ("can not find meta data client");
    }
    try {
      for (String keyspace : metaDataManager.listKeyspaces()) {
        if (keyspace.equals("system")){
          continue;
        }
        KeyspaceMetaData d = metaDataManager.getKeyspaceMetadata(keyspace);
        metaDataClient.createOrUpdateKeyspace(keyspace, d.getProperties(), false);
        for (String store : metaDataManager.listStores(keyspace)) {
          metaDataClient.createOrUpdateStore(keyspace, store, 
                  metaDataManager.getStoreMetadata(keyspace, store).getProperties());
        }
      }
    } catch (ClientException| RuntimeException e) {
      throw new RuntimeException(e);
    }
    boolean res = protege.compareAndSet(null, protegeDestination);
    //TODO check to make sure node is own range
    //TODO provide auto-token
    protogeToken.set(wantedToken);
    Thread t = new Thread(){
      public void run(){
        InternodeClient protegeClient = new InternodeClient(protogeHost, server.getConfiguration().getTransportPort());
        Keyspace ks = server.getKeyspaces().get(joinKeyspace);
        for (Entry<String, Store> storeEntry : ks.getStores().entrySet()){
          if (storeEntry.getValue() instanceof DefaultColumnFamily){
            DefaultColumnFamily d = (DefaultColumnFamily) storeEntry.getValue();
            d.doFlush();
            d.getMemtableFlusher().doBlockingFlush();
            String  bulkUid = UUID.randomUUID().toString();
            for (SsTable table : d.getSstable()){
              protegeClient.createSsTable(joinKeyspace, d.getStoreMetadata().getName(), bulkUid);
              try {
                SsTableStreamReader stream = table.getStreamReader();
                Token token = null;
                while ((token = stream.getNextToken()) != null){
                  SortedMap<AtomKey,AtomValue> columns = stream.readColumns();
                  System.out.println("sending "+token + " "+ columns);
                  protegeClient.transmit(ks.getKeyspaceMetaData().getName(), storeEntry.getKey(), token, columns, bulkUid);
                }
              } catch (IOException e) {
                throw new RuntimeException (e);
              }
              protegeClient.closeSsTable(joinKeyspace, d.getStoreMetadata().getName(), bulkUid);
            }
          }
        }
      }
    };
    t.start();
    
    
    //
    if (res){
      return new Response().withProperty("status", "ok");
    } else { 
      return new Response().withProperty("status", "fail")
              .withProperty("reason", "already sponsoring") ;
    }
  }
  
  public Destination getProtege(){
    return protege.get();
  }
  
  
}
