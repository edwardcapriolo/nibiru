package io.teknek.nibiru.cluster;

import java.util.List;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ServerId;

public abstract class ClusterMembership {

  protected Configuration configuration;
  protected ServerId serverId;
  
  public ClusterMembership(Configuration configuration, ServerId serverId){
    this.configuration = configuration;
    this.serverId = serverId;
  }
  
  public abstract void init();
  public abstract void shutdown();
  public abstract List<ClusterMember> getLiveMembers();
  public abstract List<ClusterMember> getDeadMembers();
  
}
