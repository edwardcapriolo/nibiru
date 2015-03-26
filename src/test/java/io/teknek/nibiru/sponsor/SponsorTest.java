package io.teknek.nibiru.sponsor;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ConfigurationClusterMembership;
import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.router.TokenRouter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SponsorTest {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node2Folder = new TemporaryFolder();
  
  @Test
  public void test() throws ClientException, InterruptedException{
    Server[] s = new Server[2];
    TemporaryFolder [] t = { node1Folder, node2Folder};
    Configuration [] cs = new Configuration[2];
    for (int i = 0; i < cs.length; i++) {
      cs[i] = TestUtil.aBasicConfiguration(t[i]);
      cs[i].setTransportHost("127.0.0." + (i + 1));
      cs[i].setClusterMembershipProperties(TestUtil.gossipPropertiesFor127Seed());
      s[i] = new Server(cs[i]);
    }
    s[0].init();
        
    MetaDataClient metaClient = new MetaDataClient(s[0].getConfiguration().getTransportHost(), s[0]
            .getConfiguration().getTransportPort());
    Map<String,Object> props = new HashMap<>();
    TreeMap<String,String> tokenMap = new TreeMap<>();
    tokenMap.put("0", s[0].getServerId().getU().toString());
    props.put(TokenRouter.TOKEN_MAP_KEY, tokenMap);
    props.put(TokenRouter.REPLICATION_FACTOR, 1);
    props.put(KeyspaceMetaData.ROUTER_CLASS, TokenRouter.class.getName());
    metaClient.createOrUpdateKeyspace("abc", props, true);
    
    Map <String,Object> x = new HashMap<String,Object>();
    x.put(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    metaClient.createOrUpdateStore("abc", "def", x);
    Assert.assertEquals(s[0].getClusterMembership().getLiveMembers().size(), 0);//We do not count ourselves
    for (int k = 0; k < 10; k++) {
      s[0].put("abc", "def", k + "", k + "", k + "", 1);
    }
    s[1].init(); 
    
    Thread.sleep(10000);
    Assert.assertEquals(s[0].getClusterMembership().getLiveMembers().size(), 1);
    s[1].join("abc", "127.0.0.1");
    Thread.sleep(1000);
    Assert.assertEquals(s[1].getServerId().getU().toString(), 
            s[0].getCoordinator().getSponsorCoordinator().getProtege().getDestinationId());
    
    for (int i = 0; i < cs.length; i++) {
      s[i].shutdown();
    }
    metaClient.shutdown();
  }
}
