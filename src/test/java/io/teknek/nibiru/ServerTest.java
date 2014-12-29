package io.teknek.nibiru;
import java.io.File;
import java.io.IOException;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.engine.Val;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ServerTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    File tempFolder = testFolder.newFolder("sstable");
    System.out.println("Test folder: " + testFolder.getRoot());
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    Server s = new Server(configuration);
    s.init();
    s.createKeyspace(ks);
    s.createColumnFamily(ks, cf);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    s.put(ks, cf, "jack", "name", "bunnyjack", 1);
    s.put(ks, cf, "jack", "age", "6", 1);
    Val x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    s.put(ks, cf, "ziggy", "name", "ziggyrabbit", 1);
    s.put(ks, cf, "ziggy", "age", "8", 1);
    s.put(ks, cf, "dotty", "age", "4", 1);
    Thread.sleep(2000);
    Assert.assertEquals(2, s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getMemtableFlusher().getFlushCount());
    x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    
  }
  
  @Test
  public void compactionTest() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    File tempFolder = testFolder.newFolder("sstable");
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    Server s = new Server(configuration);
    s.init();
    s.createKeyspace(ks);
    s.createColumnFamily(ks, cf);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    for (int i = 0; i < 9; i++) {
      s.put(ks, cf, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(ks, cf, "8", "age");
    Thread.sleep(1000);
    Assert.assertEquals(4, s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getMemtableFlusher().getFlushCount());
    Assert.assertEquals(1, s.getCompactionManager().getNumberOfCompactions());
    Assert.assertEquals("4", x.getValue());
    for (int i = 0; i < 9; i++) {
      Val y = s.get(ks, cf, i+"", "age");
      Assert.assertEquals("4", y.getValue());
    }
  }
  
  
  @Test
  public void commitLogTests() throws IOException, InterruptedException{
    String ks = "data";
    String cf = "pets";
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    configuration.setCommitlogDirectory(commitlog);
    Server s = new Server(configuration);
    s.init();
    s.createKeyspace(ks);
    s.createColumnFamily(ks, cf);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    System.out.println(s.getKeyspaces().get(ks).getKeyspaceMetadata());
    for (int i = 0; i < 2; i++) {
      s.put(ks, cf, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(ks, cf, "8", "age");
    Thread.sleep(1000);
    s.shutdown();
    Thread.sleep(1000);
    
    {
      Server j = new Server(configuration);
      j.init();
      Assert.assertNotNull(j.getKeyspaces().get(ks).getColumnFamilies().get(cf));
      Val y = j.get(ks, cf, "8", "age");
      Assert.assertEquals("'", y.getValue());
    }
    
  }
  
  
}