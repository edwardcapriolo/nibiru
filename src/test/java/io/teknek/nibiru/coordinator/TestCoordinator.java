package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.cluster.ConfigurationClusterMembership;
import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.personality.MetaPersonality;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class TestCoordinator {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node2Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node3Folder = new TemporaryFolder();
  
  @Test
  public void doIt() {
    Server[] s = new Server[3];
    TemporaryFolder [] t = { node1Folder, node2Folder, node3Folder }; 
    Map<String, String> payload = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      Configuration conf = TestUtil.aBasicConfiguration(t[i]);
      conf.setClusterMembershipClass(ConfigurationClusterMembership.class.getName());
      Map<String, Object> clusterProperties = new HashMap<>();
      clusterProperties.put(ConfigurationClusterMembership.HOSTS, payload);
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0." + i);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      s[i] = new Server(conf);
      s[i].init();
      payload.put("127.0.0." + i, s[i].getServerId().getU().toString());
    }
    for (int i = 0; i < 3; i++) {
      s[i].shutdown();
      s[i].init();
      
    }
  }
}
