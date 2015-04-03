package io.teknek.nibiru.coordinator;

import java.util.ArrayList;
import java.util.List;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ContactInformation;
import io.teknek.nibiru.Destination;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.personality.LocatorPersonality;
import io.teknek.nibiru.transport.Response;

public class Locator implements LocatorPersonality {

  private final Configuration configuration;
  private final ClusterMembership clusterMembership;
  
  public Locator(Configuration configuration, ClusterMembership clusterMembership){
    this.configuration = configuration;
    this.clusterMembership = clusterMembership;
  }

  @Override
  public List<ContactInformation> locateRowKey(List<Destination> destinations) {
    List<ContactInformation> contactInformation = new ArrayList<ContactInformation>();
    for (Destination destinatrion : destinations){
      contactInformation.add(new ContactInformation(destinatrion, 
              clusterMembership.findHostnameForId(destinatrion.getDestinationId())));
    }
    return contactInformation;
  }
  
  public Response locate(List<Destination> destinations){
    return new Response().withProperty("payload", locateRowKey(destinations));
  }
}
