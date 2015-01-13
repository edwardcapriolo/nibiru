package io.teknek.nibiru.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.ServerTest;
import io.teknek.nibiru.TestUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCluster {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node2Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node3Folder = new TemporaryFolder();
  
  @Test
  public void letTwoNodesDiscoverEachOther() throws InterruptedException{
    Server s1, s2, s3;
    {
      Configuration conf = TestUtil.aBasicConfiguration(node1Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0.1");
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setDataDirectory(node1Folder.getRoot());
      s1 = new Server(conf);
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node2Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.2");
      conf.setDataDirectory(node2Folder.getRoot());
      s2 = new Server(conf);
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node2Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.3");
      conf.setDataDirectory(node3Folder.getRoot());
      s3 = new Server(conf);
    }
    s1.init();
    s2.init();
    s3.init();
    Thread.sleep(11000);
    Assert.assertEquals(2 , s2.getClusterMembership().getLiveMembers().size());
    Assert.assertEquals("127.0.0.1", s2.getClusterMembership().getLiveMembers().get(0).getHost());
    System.out.println("s1 "+s1.getClusterMembership().getLiveMembers());
    System.out.println("s2 "+s2.getClusterMembership().getLiveMembers());
    System.out.println("s3 "+s3.getClusterMembership().getLiveMembers());
    s1.shutdown();
    s2.shutdown();
    s3.shutdown();
  }
}
