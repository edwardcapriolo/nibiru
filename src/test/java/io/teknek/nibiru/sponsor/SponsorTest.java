package io.teknek.nibiru.sponsor;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.router.TokenRouter;
import io.teknek.nibiru.transport.Response;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Ignore;
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
    Server[] servers = new Server[2];
    TemporaryFolder [] tempFolders = { node1Folder, node2Folder};
    Configuration [] cs = new Configuration[2];
    for (int i = 0; i < cs.length; i++) {
      cs[i] = TestUtil.aBasicConfiguration(tempFolders[i]);
      cs[i].setTransportHost("127.0.0." + (i + 1));
      cs[i].setClusterMembershipProperties(TestUtil.gossipPropertiesFor127Seed());
      servers[i] = new Server(cs[i]);
    }
    servers[0].init();
        
    MetaDataClient metaClient = new MetaDataClient(servers[0].getConfiguration().getTransportHost(), servers[0]
            .getConfiguration().getTransportPort());
    createKeyspaceInformation(metaClient, servers);    
    Assert.assertEquals(servers[0].getClusterMembership().getLiveMembers().size(), 0);//We do not count ourselves
    
    ColumnFamilyClient c = new ColumnFamilyClient(new Client(servers[0].getConfiguration().getTransportHost(), servers[0]
            .getConfiguration().getTransportPort(),10000,10000));
    Session session = c.createBuilder().withKeyspace("abc")
            .withWriteConsistency(ConsistencyLevel.ALL, new HashMap())
            .withReadConsistency(ConsistencyLevel.ALL, new HashMap())
            .withStore("def").build();
    for (int k = 0; k < 10; k++) {
      session.put(k+"", k+"", k+"", 1);
    }
    
    servers[1].init(); 
    Thread.sleep(10000);
    Assert.assertEquals(servers[0].getClusterMembership().getLiveMembers().size(), 1);
    
    servers[1].join("abc", "127.0.0.1", "5");
    Thread.sleep(1000);
    Assert.assertEquals(servers[1].getServerId().getU().toString(), 
            servers[0].getCoordinator().getSponsorCoordinator().getProtege().getDestinationId());
    
    insertDataOverClient(session);
    assertDataIsDistributed(servers);
    
    Thread.sleep(5000);
    Map<String,String> keyspaceMembers = (Map<String, String>) metaClient.getKeyspaceMetadata("abc").get(TokenRouter.TOKEN_MAP_KEY);
    Assert.assertEquals(2, keyspaceMembers.size());
    
    for (int i = 0; i < cs.length; i++) {
      servers[i].shutdown();
    }
    metaClient.shutdown();
  }

  private void createKeyspaceInformation(MetaDataClient metaClient, Server [] s) throws ClientException{
    Map<String,Object> props = new HashMap<>();
    TreeMap<String,String> tokenMap = new TreeMap<>();
    tokenMap.put("10", s[0].getServerId().getU().toString());
    props.put(TokenRouter.TOKEN_MAP_KEY, tokenMap);
    props.put(TokenRouter.REPLICATION_FACTOR, 1);
    props.put(KeyspaceMetaData.ROUTER_CLASS, TokenRouter.class.getName());
    metaClient.createOrUpdateKeyspace("abc", props, true);
    metaClient.createOrUpdateStore("abc", "def", new Response()
    .withProperty(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()));
  }
  
  private void insertDataOverClient(Session session) throws ClientException {
    session.put("1", "1", "after", 8);
    session.put("7", "7", "after", 8);
    session.put("11", "11", "after", 8);
  }
  
  private void assertDataIsDistributed(Server [] servers){
    Assert.assertEquals("after", ((ColumnValue) servers[1].get("abc", "def", "11" , "11")).getValue());
    Assert.assertEquals("after", ((ColumnValue) servers[0].get("abc", "def", "1" , "1")).getValue());
    Assert.assertEquals("after", ((ColumnValue) servers[1].get("abc", "def", "11" , "11")).getValue());
    Assert.assertEquals("after", ((ColumnValue) servers[1].get("abc", "def", "1" , "1")).getValue());
  }
  

  
}
