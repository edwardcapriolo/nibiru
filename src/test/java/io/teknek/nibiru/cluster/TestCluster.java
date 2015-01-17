package io.teknek.nibiru.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.ServerTest;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

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
  public void letTwoNodesDiscoverEachOther() throws InterruptedException, ClientException{
    Server [] s = new Server[3];
    {
      Configuration conf = TestUtil.aBasicConfiguration(node1Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0.1");
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      s[0] = new Server(conf);
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node2Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.2");
      s[1] = new Server(conf);
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node3Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.3");
      s[2] = new Server(conf);
    }
    for (Server server : s){
      server.init();
    }
    Thread.sleep(11000);
    Assert.assertEquals(2 , s[2].getClusterMembership().getLiveMembers().size());
    Assert.assertEquals("127.0.0.1", s[2].getClusterMembership().getLiveMembers().get(0).getHost());
    MetaDataClient c = new MetaDataClient("127.0.0.1", s[1].getConfiguration().getTransportPort());
    c.createOrUpdateKeyspace("abc", new HashMap<String,Object>());
    Thread.sleep(1000);
    for (Server server : s){
      Assert.assertNotNull(server.getKeyspaces().get("abc"));
    }
    Map<String,Object> stuff = new HashMap<String,Object>();
    stuff.put(ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    c.createOrUpdateColumnFamily("abc", "def", stuff);
    Thread.sleep(1000);
    for (Server server : s){
      Assert.assertNotNull(server.getKeyspaces().get("abc").getColumnFamilies().get("def"));
      
    }
    for (Server server : s){
     server.shutdown();
    }
  }
}