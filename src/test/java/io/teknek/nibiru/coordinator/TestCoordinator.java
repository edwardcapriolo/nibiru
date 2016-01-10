package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;
import io.teknek.nibiru.cluster.ConfigurationClusterMembership;
import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.plugins.HintReplayer;
import io.teknek.nibiru.router.TokenRouter;
import io.teknek.nibiru.transport.Response;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;


import org.junit.Assert;
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
  
  @Rule
  public TemporaryFolder node4Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node5Folder = new TemporaryFolder();
  
  private void createMetaData(Server [] s) throws ClientException, InterruptedException {
    MetaDataClient c = new MetaDataClient(s[0].getConfiguration().getTransportHost(), s[0]
            .getConfiguration().getTransportPort());
    Map<String,Object> props = new HashMap<>();
    TreeMap<String,String> tokenMap = new TreeMap<>();
    tokenMap.put("c", s[0].getServerId().getU().toString());
    tokenMap.put("e", s[1].getServerId().getU().toString());
    tokenMap.put("j", s[2].getServerId().getU().toString());
    tokenMap.put("k", s[3].getServerId().getU().toString());
    tokenMap.put("m", s[4].getServerId().getU().toString());
    props.put(TokenRouter.TOKEN_MAP_KEY, tokenMap);
    props.put(TokenRouter.REPLICATION_FACTOR, 3);
    props.put(KeyspaceMetaData.ROUTER_CLASS, TokenRouter.class.getName());
    c.createOrUpdateKeyspace("abc", props,true);
    Map <String,Object> x = new HashMap<String,Object>();
    x.put(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    c.createOrUpdateStore("abc", "def", x, true);
    Thread.sleep(10);
    for (int i = 0; i < s.length; i++) {
      Assert.assertTrue(s[i].getKeyspaces().containsKey("abc"));
      Assert.assertEquals("io.teknek.nibiru.router.TokenRouter", s[i].getKeyspaces().get("abc").getKeyspaceMetaData().getRouter().getClass().getName());
      Assert.assertTrue(s[i].getKeyspaces().get("abc").getStores().containsKey("def"));
    }
    
  }
  
  @Test
  public void doIt() throws ClientException, InterruptedException {
    Server[] s = new Server[5];
    TemporaryFolder [] t = { node1Folder, node2Folder, node3Folder, node4Folder, node5Folder };
    Configuration [] cs = new Configuration[5];
    Map<String, String> payload = new HashMap<>();
    for (int i = 0; i < s.length; i++) {
      Configuration conf = TestUtil.aBasicConfiguration(t[i]);
      conf.setClusterMembershipClass(ConfigurationClusterMembership.class.getName());
      Map<String, Object> clusterProperties = new HashMap<>();
      clusterProperties.put(ConfigurationClusterMembership.HOSTS, payload);
      conf.setClusterMembershipProperties(clusterProperties);
      conf.setTransportHost("127.0.0." + (i + 1));
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      cs[i] = conf;
    }
    for (int i = 0; i < s.length; i++) {
      s[i] = new Server(cs[i]);
      s[i].init();
      payload.put("127.0.0." + (i + 1), s[i].getServerId().getU().toString());
      s[i].shutdown();
    }
    for (int i = 0; i < s.length; i++) {
      s[i] = new Server(cs[i]);
      s[i].init();
    }
    createMetaData(s);
    ColumnFamilyClient cf = new ColumnFamilyClient( new Client(s[0].getConfiguration().getTransportHost(), s[0]
            .getConfiguration().getTransportPort(),10000,10000));
    Session clAll = cf.createBuilder().withKeyspace("abc").withStore("def")
            .withWriteConsistency(ConsistencyLevel.ALL, new HashMap())
            .withReadConsistency(ConsistencyLevel.ALL, new HashMap()).build();
    Map one = new HashMap(); one.put("n", 1);
    Session clOne = cf.createBuilder().withKeyspace("abc").withStore("def")
            .withReadConsistency(ConsistencyLevel.N, one).withWriteConsistency(ConsistencyLevel.N, one).build();
    
    passIfAllAreUp(s, clAll);
    passIfOneIsUp(s, clOne);
    failIfSomeAreDown(s, clAll);
    for (int i=0;i<2;i++){
      s[i].shutdown();
    }
    //test hinted handoff
    for (int i = 0; i < s.length; i++) {
      s[i] = new Server(cs[i]);
      s[i].init();
    }
    HintReplayer h = (HintReplayer) s[0].getPlugins().get(HintReplayer.MY_NAME);
    for (int tries = 0; tries < 10; tries++) {
      long x = h.getHintsDelivered();
      if (x == 3) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(3, h.getHintsDelivered());
    int found = 0;
    for (int i = 0; i < s.length; i++) {
       ColumnValue cv = (ColumnValue) s[i].get("abc", "def", "a", "b");
       if (cv != null && cv.getValue().equals("d")){
         found++;
       }
    }
    Assert.assertEquals(3, found);
    for (int i = 0; i < s.length; i++) {
      s[i].shutdown();
    }
  }
  
  public void passIfOneIsUp(Server [] s, Session sb) throws ClientException {
    Response r = sb.put("b", "b", "c", 1);
    Assert.assertFalse(r.containsKey("exception"));
    Assert.assertEquals("c", sb.get("b", "b").getValue());
  }
  
  public void passIfAllAreUp(Server [] s, Session sb) throws ClientException {
    Response r = sb.put("a", "b", "c", 1);
    Assert.assertFalse(r.containsKey("exception"));
    int found = 0;
    for (int i = 0; i < s.length; i++) {
      ColumnFamilyPersonality c = (ColumnFamilyPersonality) s[i].getKeyspaces().get("abc")
              .getStores().get("def"); 
      AtomValue v = c.get("a", "b");
      if (v != null){
        Assert.assertEquals("c", ((ColumnValue) c.get("a", "b")).getValue());
        found ++;
      }
      Assert.assertEquals(0, s[i].getCoordinator().getHinter().getHintsAdded());
    }
    Assert.assertEquals(3, found);
    Assert.assertEquals("c", sb.get("a", "b").getValue());
    
  }
  
  public void failIfSomeAreDown(Server [] s, Session sb) throws ClientException {
    for (int i = 2; i < s.length; i++) {
      s[i].shutdown();
      try {
        sb.put("a", "b", "d", 2);
        Assert.assertEquals(1, s[i].getCoordinator().getHinter().getHintsAdded());
      } catch (ClientException ex){
        Assert.assertEquals("coordinator timeout", ex.getMessage());
      }
    }
  }
}
