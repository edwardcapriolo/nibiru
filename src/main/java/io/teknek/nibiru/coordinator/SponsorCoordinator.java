package io.teknek.nibiru.coordinator;

import java.util.concurrent.atomic.AtomicReference;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.MetaDataManager;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class SponsorCoordinator {

  private final ClusterMembership clusterMembership;
  private final MetaDataManager metaDataManager;
  private final MetaDataCoordinator metaDataCoordinator;
  private final AtomicReference<Destination> protege;
  
  public SponsorCoordinator(ClusterMembership clusterMembership,
          MetaDataManager metaDataManager, MetaDataCoordinator metaDataCoordinator) {
    this.clusterMembership = clusterMembership;
    this.metaDataManager = metaDataManager;
    this.metaDataCoordinator = metaDataCoordinator;
    this.protege = new AtomicReference<>();
  }
  
  public Response handleSponsorRequest(Message message){
    
    String requestId = (String) message.getPayload().get("request_id");
    String joinKeyspace = (String) message.getPayload().get("keyspace");
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
        KeyspaceMetaData d = metaDataManager.getKeyspaceMetadata(keyspace);
        metaDataClient.createOrUpdateKeyspace(keyspace, d.getProperties(), false);
        for (String store : metaDataManager.listStores(keyspace)) {
          metaDataClient.createOrUpdateStore(keyspace, store, 
                  metaDataManager.getStoreMetadata(keyspace, store).getProperties());
        }
      }
    } catch (ClientException| RuntimeException e) {
      e.printStackTrace();
      
    }
    boolean res = protege.compareAndSet(null, protegeDestination);
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
