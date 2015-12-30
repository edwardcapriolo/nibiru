package io.teknek.nibiru.engine;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.engine.Memtable;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.TombstoneValue;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.partitioner.Md5Partitioner;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.transport.Response;

import java.util.HashMap;
import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MemtableTest extends AbstractMemtableTest {
  
  @Override
  public AbstractMemtable makeMemtable(Keyspace ks1) {
    Memtable m = new Memtable( ks1.getStores().get("abc"), new CommitLog(ks1.getStores().get("abc")));
    return m;
  }

  public static Keyspace keyspaceWithNaturalPartitioner(TemporaryFolder testFolder){
    Keyspace ks1 = new Keyspace(SSTableTest.getBasicConfiguration(testFolder));
    ks1.setKeyspaceMetadata(new KeyspaceMetaData("testks", new HashMap<String,Object>()));
    ks1.getKeyspaceMetaData().setPartitioner(new NaturalPartitioner());
    return ks1;
  }
  
  private Keyspace keyspaceWithMd5Partitioner(){
    Keyspace ks1 = new Keyspace(new Configuration());
    ks1.setKeyspaceMetadata(new KeyspaceMetaData("testks", new HashMap<String,Object>()));
    ks1.getKeyspaceMetaData().setPartitioner(new Md5Partitioner());
    return ks1;
  }
  
  @Test
  public void test(){
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()));
    AbstractMemtable m = makeMemtable(ks1);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", ((ColumnValue)m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2")).getValue());
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    Assert.assertEquals("d", ((ColumnValue)m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2")).getValue());
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "c", "d", 1, 0L);
    SortedMap<AtomKey, AtomValue> results =  m.slice(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "a", "z");
    TestUtil.compareColumnValue(new ColumnValue("d", 2,0,0), results.get(new ColumnKey("column2")));
    TestUtil.compareColumnValue(new ColumnValue("d", 1,0,0), results.get(new ColumnKey("c")));
    
  }
    
  @Test
  public void aSliceWithTomb(){
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()));
    Memtable m = new Memtable(ks1.getStores().get("abc"), new CommitLog(ks1.getStores().get("abc")));
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1L , 0L);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column3", "d", 4L, 0L);
    m.delete(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), 3);
    SortedMap<AtomKey, AtomValue> result = m.slice(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "a", "z");
    Assert.assertTrue( result.get(result.firstKey()) instanceof TombstoneValue );
    TestUtil.compareColumnValue(new ColumnValue("d", 4, 0, 0), result.get(result.lastKey()));
  }
}
