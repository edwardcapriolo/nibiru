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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.code.gossip.GossipMember;
import com.google.code.gossip.GossipService;
import com.google.code.gossip.GossipSettings;
import com.google.code.gossip.LocalGossipMember;
import com.google.code.gossip.RemoteGossipMember;
import com.google.code.gossip.event.GossipListener;
import com.google.code.gossip.event.GossipState;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ServerId;

public class GossipClusterMembership extends ClusterMembership{

  static final Logger LOGGER = Logger.getLogger(GossipClusterMembership.class);
  public static final String HOSTS = "gossip_hosts_list";
  public static final String PORT = "gossip_port";
  public static final int GOSSIP_PORT = 2000;
  
  private GossipService gossipService;
  
  public GossipClusterMembership(Configuration configuration, ServerId serverId) {
    super(configuration, serverId);
  }

  class LogGossipListener implements GossipListener {
    @Override
    public void gossipEvent(GossipMember member, GossipState state) {
      LOGGER.debug(serverId.getU() + " " + member + state);     
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void init() {
    ArrayList<GossipMember> startupMembers = new ArrayList<GossipMember>();
    List<String> hosts = new ArrayList<String>();
    if (configuration.getClusterMembershipProperties() != null)
      hosts = (List<String>) configuration.getClusterMembershipProperties().get(HOSTS);
    for (String host : hosts)
      startupMembers.add(new RemoteGossipMember(host, GOSSIP_PORT, ""));
    try {
      gossipService = new GossipService(configuration.getTransportHost(), GOSSIP_PORT, serverId
              .getU().toString(), startupMembers, new GossipSettings(), new LogGossipListener());
    } catch (UnknownHostException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    gossipService.start();
  }

  @Override
  public void shutdown() {
    try {
      gossipService.shutdown(); 
    } catch (RuntimeException ex) {
      System.err.println(ex);
    }
  }

  @Override
  public List<ClusterMember> getLiveMembers() {
    List<ClusterMember> results = new ArrayList<ClusterMember>();
    List<LocalGossipMember> m = gossipService.get_gossipManager().getMemberList();
    for (LocalGossipMember member: m){
      ClusterMember thisRow = new ClusterMember();
      thisRow.setHost(member.getHost());
      thisRow.setPort(member.getPort());
      thisRow.setHeatbeat(member.getHeartbeat());
      thisRow.setId(member.getId());
      results.add(thisRow);
    }
    return results;
  }

  @Override
  public List<ClusterMember> getDeadMembers() {
    List<ClusterMember> results = new ArrayList<ClusterMember>();
    List<LocalGossipMember> m = gossipService.get_gossipManager().getDeadList();
    for (LocalGossipMember member: m){
      ClusterMember thisRow = new ClusterMember();
      thisRow.setHost(member.getHost());
      thisRow.setPort(member.getPort());
      thisRow.setHeatbeat(member.getHeartbeat());
      
      results.add(thisRow);
    }
    return results;
  }

}
