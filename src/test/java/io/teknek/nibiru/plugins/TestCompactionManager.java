package io.teknek.nibiru.plugins;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;

import java.io.IOException;
import java.util.HashMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCompactionManager {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void compactionTest() throws IOException, InterruptedException{
    Server s = TestUtil.aBasicServer(testFolder);
    s.getMetaDataManager().createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, new HashMap<String,Object>());
    s.getMetaDataManager().createOrUpdateColumnFamily(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY);
    s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY).getColumnFamilyMetadata().setFlushNumberOfRowKeys(2);
    for (int i = 0; i < 9; i++) {
      s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age", "4", 1);
      Thread.sleep(1);
    }
    AtomValue x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "8", "age");
    Thread.sleep(1000);
    Assert.assertEquals(4, ((DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getColumnFamilies().get(TestUtil.PETS_COLUMN_FAMILY))
            .getMemtableFlusher().getFlushCount());
    Assert.assertEquals(1, ((CompactionManager) s.getPlugins().get(CompactionManager.MY_NAME)).getNumberOfCompactions());
    Assert.assertEquals("4", ((ColumnValue) x).getValue());
    for (int i = 0; i < 9; i++) {
      AtomValue y = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, i+"", "age");
      Assert.assertEquals("4", ((ColumnValue) y).getValue());
    }
    s.shutdown();
  }
}
