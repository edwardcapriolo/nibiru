package io.teknek.nibiru.sponsor;

import java.util.SortedMap;
import java.util.TreeMap;

import junit.framework.Assert;
import io.teknek.nibiru.AbstractTestServer;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.client.InternodeClient;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;

import org.junit.Test;

public class BulkWriterTest extends AbstractTestServer {

  @Test
  public void aTest(){
    InternodeClient i = new InternodeClient(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort());
    i.createSsTable(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, 1+"");
    DefaultColumnFamily store = (DefaultColumnFamily) server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores().get(TestUtil.PETS_COLUMN_FAMILY);
    Assert.assertEquals(1, store.getStreamSessions().size());
    Token t = new Token("a", "a");
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
    Assert.assertEquals("b", ((ColumnValue) server.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "a", "a")).getValue());
  }
}
