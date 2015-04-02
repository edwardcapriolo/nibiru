package io.teknek.nibiru.sponsor;

import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.InternodeClient;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BulkWriterTest {
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest(){
    Server s = TestUtil.aBasicServer(testFolder);
    InternodeClient i = new InternodeClient(s.getConfiguration().getTransportHost(), 
            s.getConfiguration().getTransportPort());
    i.createSsTable(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, 1+"");
    DefaultColumnFamily store = (DefaultColumnFamily) s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY);
    Assert.assertEquals(1, store.getStreamSessions().size());
    Token t = new Token();
    t.setRowkey("a");
    t.setToken("a");
    SortedMap<AtomKey,AtomValue> a= new TreeMap<>();
    ColumnKey k = new ColumnKey("a") ;
    ColumnValue v = new ColumnValue();
    v.setCreateTime(1);
    v.setTime(1);
    v.setValue("b");
    a.put(k, v);
    i.transmit(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, t, a, 1+"");
    i.closeSsTable(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, 1+"");
    Assert.assertEquals(1, store.getSstable().size());
    Assert.assertEquals(0, store.getStreamSessions().size());
    Assert.assertEquals("b", ((ColumnValue) s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "a", "a")).getValue());
    s.shutdown();
  }
}
