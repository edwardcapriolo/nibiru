package io.teknek.nibiru.cluster;

import java.util.List;

import io.teknek.nibiru.Configuration;

public abstract class ClusterMembership {

  protected Configuration configuration;
  
  public ClusterMembership(Configuration configuration){
    this.configuration = configuration;  
  }
  
  public abstract void init();
  public abstract void shutdown();
  public abstract List<ClusterMember> getLiveMembers();
  public abstract List<ClusterMember> getDeadMembers();
  
}
