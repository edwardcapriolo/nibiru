package io.teknek.nibiru.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


import io.teknek.nibiru.ServerShutdown;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.tunit.TUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;

public class TestCluster extends ServerShutdown {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node2Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node3Folder = new TemporaryFolder();
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void letTwoNodesDiscoverEachOther() throws InterruptedException, ClientException{
    final Server [] s = new Server[3];
    {
      Configuration conf = TestUtil.aBasicConfiguration(node1Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0.1");
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      s[0] = registerServer(new Server(conf));
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node2Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.2");
      s[1] = registerServer(new Server(conf));
    }
    {
      Configuration conf = TestUtil.aBasicConfiguration(node3Folder);
      Map<String,Object> clusterProperties = new HashMap<>();
      conf.setClusterMembershipProperties(clusterProperties);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      conf.setTransportHost("127.0.0.3");
      s[2] = registerServer(new Server(conf));
    }
    for (Server server : s){
      server.init();
    }
    TUnit.assertThat( new Callable(){
      public Object call() throws Exception {
        return s[2].getClusterMembership().getLiveMembers().size();
      }}).afterWaitingAtMost(11, TimeUnit.SECONDS).isEqualTo(2);
    Assert.assertEquals(2 , s[2].getClusterMembership().getLiveMembers().size());
    Assert.assertEquals("127.0.0.1", s[2].getClusterMembership().getLiveMembers().get(0).getHost());
    MetaDataClient c = new MetaDataClient("127.0.0.1", s[1].getConfiguration().getTransportPort(), 20000, 20000);
    c.createOrUpdateKeyspace("abc", new HashMap<String,Object>(), true);
    for (final Server server : s) {
      TUnit.assertThat(new Callable() {
        public Object call() throws Exception {
          return server.getKeyspaces().get("abc") != null;
        }
      }).afterWaitingAtMost(1000, TimeUnit.MILLISECONDS).isEqualTo(true);
    }
    Map<String,Object> stuff = new HashMap<String,Object>();
    stuff.put(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    c.createOrUpdateStore("abc", "def", stuff, true);
    Thread.sleep(1000);
    for (Server server : s){
      Assert.assertNotNull(server.getKeyspaces().get("abc").getStores().get("def"));
      Set<String> livingHosts = new TreeSet<>();
      for (ClusterMember cm : c.getLiveMembers()){
        livingHosts.add(cm.getHost());
      }
      Assert.assertEquals(Sets.newHashSet("127.0.0.1", "127.0.0.2", "127.0.0.3"), livingHosts);
    }
  }
}
