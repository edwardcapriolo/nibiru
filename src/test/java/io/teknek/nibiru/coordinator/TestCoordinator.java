package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;

import io.teknek.nibiru.cluster.ConfigurationClusterMembership;
import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.personality.MetaPersonality;
import io.teknek.nibiru.router.TokenRouter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

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
  
  @Rule
  public TemporaryFolder node4Folder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder node5Folder = new TemporaryFolder();
  
  
  @Test
  public void doIt() throws ClientException {
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
      conf.setTransportHost("127.0.0." + i+1);
      clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
      cs[i] = conf;
    }
    for (int i = 0; i < s.length; i++) {
      s[i] = new Server(cs[i]);
      s[i].init();
      payload.put("127.0.0." + i+1, s[i].getServerId().getU().toString());
      s[i].shutdown();
    }
    for (int i = 0; i < s.length; i++) {
      s[i] = new Server(cs[i]);
      s[i].init();
    }
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
    c.createOrUpdateKeyspace("abc", props);
    Map <String,Object> x = new HashMap<String,Object>();
    x.put(ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
    c.createOrUpdateColumnFamily("abc", "def", x);
    ColumnFamilyClient cf = new ColumnFamilyClient(s[0].getConfiguration().getTransportHost(), s[0]
            .getConfiguration().getTransportPort());
    Session sb = cf.createBuilder().withKeyspace("abc").withColumnFamily("def")
            .withWriteConsistency(ConsistencyLevel.ALL, new HashMap()).build();
    sb.put("a", "b", "c", 1);
    //System.out.println(sb.get("a", "b"));
    //Assert.assertEquals("c", sb.get("a", "b").getValue());
    for (int i = 0; i < s.length; i++) {
      Assert.assertTrue(s[i].getKeyspaces().containsKey("abc"));
      Assert.assertEquals("io.teknek.nibiru.router.TokenRouter", s[i].getKeyspaces().get("abc").getKeyspaceMetadata().getRouter().getClass().getName());
      Assert.assertTrue(s[i].getKeyspaces().get("abc").getColumnFamilies().containsKey("def"));
    }
    for (int i = 0; i < s.length; i++) {
      s[i].shutdown();
    }
  }
}
