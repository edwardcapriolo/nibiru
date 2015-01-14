package io.teknek.nibiru;
import java.io.IOException;
import java.util.UUID;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.engine.DefaultColumnFamily;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


public class ServerTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException, InterruptedException{
    Server s = TestUtil.aBasicServer(testFolder);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name", "bunnyjack", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age", "6", 1);
    Val x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "ziggy", "name", "ziggyrabbit", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "ziggy", "age", "8", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "dotty", "age", "4", 1);
    Thread.sleep(2000);
    Assert.assertEquals(2, ((DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY))
            .getMemtableFlusher().getFlushCount());
    x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    s.shutdown();
  }
  
  @Test
  public void compactionTest() throws IOException, InterruptedException{
    Server s = TestUtil.aBasicServer(testFolder);
    s.getMetaDataManager().createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, null);
    s.getMetaDataManager().createColumnFamily(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    for (int i = 0; i < 9; i++) {
      s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "8", "age");
    Thread.sleep(1000);
    Assert.assertEquals(4, ((DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY))
            .getMemtableFlusher().getFlushCount());
    Assert.assertEquals(1, s.getCompactionManager().getNumberOfCompactions());
    Assert.assertEquals("4", x.getValue());
    for (int i = 0; i < 9; i++) {
      Val y = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age");
      Assert.assertEquals("4", y.getValue());
    }
    s.shutdown();
  }
  
  @Test
  public void serverIdTest() {
    UUID u1, u2;
    {
      Server s = TestUtil.aBasicServer(testFolder);
      u1 = s.getServerId().getU();
      s.shutdown();
    }
    {
      Server j = TestUtil.aBasicServer(testFolder);
      u2 = j.getServerId().getU();
      j.shutdown();
    }
    Assert.assertEquals(u1, u2);
  }
  
  @Test
  public void commitLogTests() throws IOException, InterruptedException{
    Configuration configuration = TestUtil.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, null);
    s.getMetaDataManager().createColumnFamily(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY).getColumnFamilyMetadata().setCommitlogFlushBytes(1);
    for (int i = 0; i < 3; i++) {
      s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    Val x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "0", "age");
    Assert.assertEquals("4", x.getValue());
    {
      Val y = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "2", "age");
      Assert.assertEquals("4", y.getValue());
    }
    Thread.sleep(1000);
    s.shutdown();
    Thread.sleep(1000);
    {
      Server j = new Server(configuration);
      j.init();
      Assert.assertNotNull(j.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY));
      Val y = j.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "2", "age");
      Assert.assertEquals("4", y.getValue());
      j.shutdown();
    }
    s.shutdown();
  }
  
  
}