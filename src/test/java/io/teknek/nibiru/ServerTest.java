package io.teknek.nibiru;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;

import io.teknek.nibiru.engine.atom.*;


public class ServerTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException, InterruptedException{
    Server s = TestUtil.aBasicServer(testFolder, 9004);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().setFlushNumberOfRowKeys(2);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name", "bunnyjack", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age", "6", 1);
    AtomValue x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", ((ColumnValue) x).getValue());
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "ziggy", "name", "ziggyrabbit", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "ziggy", "age", "8", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "dotty", "age", "4", 1);
    Thread.sleep(2000);
    Assert.assertEquals(2, ((DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY))
            .getMemtableFlusher().getFlushCount());
    x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", ((ColumnValue) x).getValue());
    Assert.assertEquals(Sets.newHashSet("system", TestUtil.DATA_KEYSPACE), s.getMetaDataManager().listKeyspaces());
    Assert.assertEquals(Sets.newHashSet(TestUtil.PETS_COLUMN_FAMILY, TestUtil.BOOKS_KEY_VALUE), s.getMetaDataManager().listStores(TestUtil.DATA_KEYSPACE));
    s.shutdown();
  }
  
  
  @Test
  public void serverIdTest() {
    UUID u1, u2;
    {
      Server s = TestUtil.aBasicServer(testFolder, 9006);
      u1 = s.getServerId().getU();
      s.shutdown();
    }
    {
      Server j = TestUtil.aBasicServer(testFolder, 9006);
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
    s.getMetaDataManager().createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, new HashMap<String,Object>());
    s.getMetaDataManager().createOrUpdateStore(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY());
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().setFlushNumberOfRowKeys(2);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().setCommitlogFlushBytes(1);
    for (int i = 0; i < 3; i++) {
      s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    {
      AtomValue y = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "2", "age");
      Assert.assertEquals("4", ((ColumnValue) y).getValue());
    }
    Thread.sleep(1000);
    s.shutdown();
    Thread.sleep(1000);
    {
      Server j = new Server(configuration);
      j.init();
      Assert.assertNotNull(j.getKeyspaces().get(TestUtil.DATA_KEYSPACE)
              .getStores().get(TestUtil.PETS_COLUMN_FAMILY));
      AtomValue y = j.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "2", "age");
      Assert.assertEquals("4", ((ColumnValue) y).getValue());
      j.shutdown();
    }
  }
  
  
}