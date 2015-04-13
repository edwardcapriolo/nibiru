package io.teknek.nibiru.plugins;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyAdminClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.router.Router;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCompactionManager {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  private Server server;
  
  @Before
  public void buildServer(){
    server = TestUtil.aBasicServer(testFolder);
  }
  
  @After
  public void closeServer(){
    server.shutdown();
  }
  
  @Test
  public void compactionTest() throws IOException, InterruptedException{
    server.getMetaDataManager().createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, new HashMap<String,Object>());
    server.getMetaDataManager().createOrUpdateStore(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY());
    server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().setFlushNumberOfRowKeys(2);
    for (int i = 0; i < 9; i++) {
      server.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    AtomValue x = server.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "8", "age");
    Thread.sleep(1000);
    Assert.assertEquals(4, ((DefaultColumnFamily) server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY))
            .getMemtableFlusher().getFlushCount());
    Assert.assertEquals(1, ((CompactionManager) server.getPlugins().get(CompactionManager.MY_NAME)).getNumberOfCompactions());
    Assert.assertEquals("4", ((ColumnValue) x).getValue());
    for (int i = 0; i < 9; i++) {
      AtomValue y = server.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age");
      Assert.assertEquals("4", ((ColumnValue) y).getValue());
    }
  }
    
  @Test
  public void cleanupTest() throws IOException, InterruptedException, ClientException {
    for (int i = 0; i < 9; i++) {
      server.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
    }
    forceFlushAndConfirmFilesOnDisk(server);
    changeTheRouter(server);
    assertSomeDatum(server);
    runCleanup(server);
    assertDatumAfterCompaction(server);
  }
  
  private void runCleanup(Server s) throws ClientException {
    boolean local = false;
    if (local) {
      CompactionManager cm = ((CompactionManager) s.getPlugins().get(CompactionManager.MY_NAME));
      cm.cleanupCompaction(
              s.getKeyspaces().get(TestUtil.DATA_KEYSPACE),
              (DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores()
                      .get(TestUtil.PETS_COLUMN_FAMILY));
    } else {
      ColumnFamilyAdminClient c = new ColumnFamilyAdminClient(s.getConfiguration()
              .getTransportHost(), s.getConfiguration().getTransportPort());
      c.cleanup(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY);
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
      }
      c.shutdown();
    }
  }
  
  private void changeTheRouter(Server s) throws ClientException{
    MetaDataClient metaDataClient = new MetaDataClient(s.getConfiguration().getTransportHost(), s
            .getConfiguration().getTransportPort());
    metaDataClient.createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, 
            new Response().withProperty(KeyspaceMetaData.ROUTER_CLASS, OnlyTheBestRouter.class.getName()), true);
    metaDataClient.shutdown();
  }
  
  private void forceFlushAndConfirmFilesOnDisk(Server s){
    DefaultColumnFamily df = (DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE)
            .getStores().get(TestUtil.PETS_COLUMN_FAMILY);
    df.doFlush();
    df.getMemtableFlusher().doBlockingFlush();
    Assert.assertEquals(true , df.getSstable().size() > 0);
  }
  
  private void assertSomeDatum(Server s){
    String res = ((ColumnValue) s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "3", "age")).getValue();
    Assert.assertEquals("4", res);
  }
  
  private void assertDatumAfterCompaction(Server s){
    Assert.assertEquals(null, s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "3", "age"));
    String res = ((ColumnValue) s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "1", "age")).getValue();
    Assert.assertEquals("4", res);
  }
  
  public static class OnlyTheBestRouter implements Router {
    @Override
    public List<Destination> routesTo(ServerId local, Keyspace requestKeyspace,
            ClusterMembership clusterMembership, Token token) {
      if (token.getRowkey().equals("1")){
        Destination d = new Destination();
        d.setDestinationId(local.getU().toString());
        return Arrays.asList(d);
      } 
      return Arrays.asList();
    }
  }
}
