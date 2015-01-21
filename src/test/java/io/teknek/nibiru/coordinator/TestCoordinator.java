package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.cluster.GossipClusterMembership;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCoordinator {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node2Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node3Folder = new TemporaryFolder();
  
  @Test
  public void doIt(){
    Server [] s = new Server[3];
    for (int i=0;i<3;i++){
      Configuration conf = TestUtil.aBasicConfiguration(node1Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0."+i);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      s[0] = new Server(conf);
    }
  }
}
