package io.teknek.nibiru;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ServerTest {

  public static String ks = "data";
  public static String cf = "pets";
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    Configuration configuration = aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    s.put(ks, cf, "jack", "name", "bunnyjack", 1);
    s.put(ks, cf, "jack", "age", "6", 1);
    Val x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    s.put(ks, cf, "ziggy", "name", "ziggyrabbit", 1);
    s.put(ks, cf, "ziggy", "age", "8", 1);
    s.put(ks, cf, "dotty", "age", "4", 1);
    Thread.sleep(2000);
    Assert.assertEquals(2, ((DefaultColumnFamily) s.getKeyspaces().get(ks).getColumnFamilies().get(cf))
            .getMemtableFlusher().getFlushCount());
    x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    s.shutdown();
  }
  
  @Test
  public void compactionTest() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    Configuration configuration = aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    for (int i = 0; i < 9; i++) {
      s.put(ks, cf, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(ks, cf, "8", "age");
    Thread.sleep(1000);
    Assert.assertEquals(4, ((DefaultColumnFamily) s.getKeyspaces().get(ks).getColumnFamilies().get(cf))
            .getMemtableFlusher().getFlushCount());
    Assert.assertEquals(1, s.getCompactionManager().getNumberOfCompactions());
    Assert.assertEquals("4", x.getValue());
    for (int i = 0; i < 9; i++) {
      Val y = s.get(ks, cf, i+"", "age");
      Assert.assertEquals("4", y.getValue());
    }
    s.shutdown();
  }
  
  @Test
  public void serverIdTest() {
    UUID u1, u2;
    {
      Server s = aBasicServer(testFolder);
      u1 = s.getServerId().getU();
      s.shutdown();
    }
    {
      Server j = aBasicServer(testFolder);
      u2 = j.getServerId().getU();
      j.shutdown();
    }
    Assert.assertEquals(u1, u2);
  }
  
  @Test
  public void commitLogTests() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    Configuration configuration = aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setCommitlogFlushBytes(1);
    for (int i = 0; i < 3; i++) {
      s.put(ks, cf, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(ks, cf, "0", "age");
    Assert.assertEquals("4", x.getValue());
    {
      Val y = s.get(ks, cf, "2", "age");
      Assert.assertEquals("4", y.getValue());
    }
    Thread.sleep(1000);
    s.shutdown();
    Thread.sleep(1000);
    {
      Server j = new Server(configuration);
      j.init();
      Assert.assertNotNull(j.getKeyspaces().get(ks).getColumnFamilies().get(cf));
      Val y = j.get(ks, cf, "2", "age");
      Assert.assertEquals("4", y.getValue());
      j.shutdown();
    }
    s.shutdown();
  }
  
  public static Configuration aBasicConfiguration(TemporaryFolder testFolder){
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration configuration = new Configuration();
    configuration.setDataDirectory(tempFolder);
    configuration.setCommitlogDirectory(commitlog);
    return configuration;
  }
  
  public static Server aBasicServer(TemporaryFolder testFolder){
    String ks = "data";
    String cf = "pets";
    Configuration configuration = ServerTest.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    return s;
  }
  
  
}