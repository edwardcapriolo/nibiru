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
package io.teknek.nibiru.cluster;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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
  
  public String findHostnameForId(String id){
    if (id.equalsIgnoreCase(serverId.getU().toString())){
      return configuration.getTransportHost();
    }
    for (ClusterMember cm : getLiveMembers()){
      if (id.equals(cm.getId())){
        return cm.getHost();
      }
    }
    for (ClusterMember cm : getDeadMembers()){
      if (id.equals(cm.getId())){
        return cm.getHost();
      }
    }
    return null;
  }
  
  public static ClusterMembership createFrom(Configuration configuration, ServerId serverId){
    try {
      Constructor<?> cons = Class.forName(configuration.getClusterMembershipClass()).getConstructor(Configuration.class, ServerId.class);
      return (ClusterMembership) cons.newInstance(configuration, serverId);
    } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
