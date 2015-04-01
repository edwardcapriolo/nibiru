package io.teknek.nibiru.sponsor;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.router.TokenRouter;

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
    tokenMap.put("10", s[0].getServerId().getU().toString());
    props.put(TokenRouter.TOKEN_MAP_KEY, tokenMap);
    props.put(TokenRouter.REPLICATION_FACTOR, 1);
    props.put(KeyspaceMetaData.ROUTER_CLASS, TokenRouter.class.getName());
    metaClient.createOrUpdateKeyspace("abc", props, true);
    
    Map <String,Object> x = new HashMap<String,Object>();
    x.put(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    metaClient.createOrUpdateStore("abc", "def", x);
    Assert.assertEquals(s[0].getClusterMembership().getLiveMembers().size(), 0);//We do not count ourselves
    
    ColumnFamilyClient c = new ColumnFamilyClient(s[0].getConfiguration().getTransportHost(), s[0]
            .getConfiguration().getTransportPort());
    Session session = c.createBuilder().withKeyspace("abc")
            .withWriteConsistency(ConsistencyLevel.ALL, new HashMap())
            .withReadConsistency(ConsistencyLevel.ALL, new HashMap())
            .withStore("def").build();
    for (int k = 0; k < 10; k++) {
      session.put(k+"", k+"", k+"", 1);
    }
    Assert.assertEquals(10, ((DefaultColumnFamily) s[0].getKeyspaces().get("abc").getStores().get("def")).getMemtable().size());
    System.out.println("starting second node");
    s[1].init(); 
    
    Thread.sleep(10000);
    Assert.assertEquals(s[0].getClusterMembership().getLiveMembers().size(), 1);
    System.out.println("joining second node");
    s[1].join("abc", "127.0.0.1", "5");
    Thread.sleep(1000);
    Assert.assertEquals(s[1].getServerId().getU().toString(), 
            s[0].getCoordinator().getSponsorCoordinator().getProtege().getDestinationId());
    
    
    
    session.put("1", "1", "after", 8);
    session.put("7", "7", "after", 8);
    session.put("11", "11", "after", 8);
    
    Assert.assertEquals("after", ((ColumnValue) s[1].get("abc", "def", "11" , "11")).getValue());
    Assert.assertEquals("after", ((ColumnValue) s[0].get("abc", "def", "1" , "1")).getValue());
    
    Assert.assertEquals("after", ((ColumnValue) s[1].get("abc", "def", "11" , "11")).getValue());
    Assert.assertEquals("after", ((ColumnValue) s[1].get("abc", "def", "1" , "1")).getValue());
    
    Thread.sleep(5000);
    Map<String,String> keyspaceMembers = (Map<String, String>) metaClient.getKeyspaceMetadata("abc").get(TokenRouter.TOKEN_MAP_KEY);
    Assert.assertEquals(2, keyspaceMembers.size());
    for (int i = 0; i < cs.length; i++) {
      s[i].shutdown();
    }
    metaClient.shutdown();
  }
}
