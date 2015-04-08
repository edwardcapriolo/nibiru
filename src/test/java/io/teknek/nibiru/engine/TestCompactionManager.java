package io.teknek.nibiru.engine;

import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.plugins.CompactionManager;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class TestCompactionManager {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void test() throws IOException{
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()));
    SsTable s = new SsTable(ks1.getStores().get("abc"));
    {
      Memtable m = new Memtable(ks1.getStores().get("abc"), new CommitLog(ks1.getStores().get("abc")));
      m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
      m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
      m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
      m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
      SSTableWriter w = new SSTableWriter();
      w.flushToDisk("1", ks1.getStores().get("abc"), m);
      s.open("1", ks1.getConfiguration());
    }
    SsTable s2 = new SsTable(ks1.getStores().get("abc"));
    {
      Memtable m2 = new Memtable(ks1.getStores().get("abc"), new CommitLog(ks1.getStores().get("abc")));
      m2.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column1", "c", 1, 0L);
      m2.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row2"), "column1", "f", 3, 0L);
      SSTableWriter w2 = new SSTableWriter();
      w2.flushToDisk("2", ks1.getStores().get("abc"), m2);
      s2.open("2", ks1.getConfiguration());
    }
    CompactionManager.compact(new SsTable[] { s, s2 }, "3", null, null, false, null);
    SsTable ss = new SsTable(ks1.getStores().get("abc"));
    ss.open("3", ks1.getConfiguration());
    Assert.assertEquals("e", ((ColumnValue) ss.get(   ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column3")).getValue());
    
  }
}
