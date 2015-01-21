package io.teknek.nibiru.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ServerId;

public class ConfigurationClusterMembership extends ClusterMembership {

  public static String HOSTS = "hosts";
  
  private List<ClusterMember> live;
  
  public ConfigurationClusterMembership(Configuration configuration, ServerId serverId) {
    super(configuration, serverId);
  }

  @Override
  public void init() {
    live = new ArrayList<ClusterMember>();
    Map<String,String> hosts = null;
    if (configuration.getClusterMembershipProperties() != null){
      hosts = (Map<String,String>) configuration.getClusterMembershipProperties().get(HOSTS);
    } else {
      hosts = new HashMap<>();
    }
    for (Map.Entry<String, String> entry : hosts.entrySet()){
      ClusterMember cm = new ClusterMember();
      cm.setHost(entry.getKey());
      cm.setId(entry.getValue());
      cm.setPort(0);
      live.add(cm);
    }
  }

  @Override
  public void shutdown() {

  }

  @Override
  public List<ClusterMember> getLiveMembers() {
    return live;
  }

  @Override
  public List<ClusterMember> getDeadMembers() {
    return new ArrayList<>();
  }

}
