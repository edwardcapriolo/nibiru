package io.teknek.nibiru.engine;

import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.TombstoneValue;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.transport.Response;

import java.util.HashMap;
import java.util.SortedMap;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@Ignore
public abstract class AbstractMemtableTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  
  public static Keyspace keyspaceWithNaturalPartitioner(TemporaryFolder testFolder){
    Keyspace ks1 = new Keyspace(SSTableTest.getBasicConfiguration(testFolder));
    ks1.setKeyspaceMetadata(new KeyspaceMetaData("testks", new HashMap<String,Object>()));
    ks1.getKeyspaceMetaData().setPartitioner(new NaturalPartitioner());
    return ks1;
  }
  
  public abstract AbstractMemtable makeMemtable(Keyspace ks1);
  
  @Test
  public void putGetTest(){
    Keyspace ks1 = AbstractMemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()));
    AbstractMemtable m = makeMemtable(ks1);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", ((ColumnValue)m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2")).getValue());
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    Assert.assertEquals("d", ((ColumnValue)m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2")).getValue());
  }
  
  @Ignore
  @Test
  public void lotsOfInsertsTest(){
    Keyspace ks1 = AbstractMemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()));
    AbstractMemtable m = makeMemtable(ks1);
    {
      long start = System.currentTimeMillis();
      for (int i = 0 ; i < 1000000; i++){
        m.put(ks1.getKeyspaceMetaData().getPartitioner().partition(String.valueOf(i)), "column2", "c", 1, 0L);
      }
      System.out.println("10000 inserts" + (System.currentTimeMillis() - start));
    }
    {
      long start = System.currentTimeMillis();
      for (int i = 0 ; i < 1000000; i++){
        //m.put(ks1.getKeyspaceMetaData().getPartitioner().partition(String.valueOf(i)), "column2", "c", 1, 0L);
        AtomValue o = m.get(ks1.getKeyspaceMetaData().getPartitioner().partition(String.valueOf(i)), "column2");
        Assert.assertNotNull(o);
        
      }
      System.out.println("10000 gets" + (System.currentTimeMillis() - start));
    }
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
  public void testRowDelete(){
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()));
    AbstractMemtable m = makeMemtable(ks1);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1, 0l);
    m.delete(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), 2);
    Assert.assertTrue(m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2") instanceof TombstoneValue);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 3, 0L);
    TestUtil.compareColumnValue( new ColumnValue("c",3, System.currentTimeMillis(), 0), 
            m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2"));
  }
  
  @Test
  public void testDeleting(){
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createStore("abc", new Response()
    .withProperty(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()));
    AbstractMemtable m = makeMemtable(ks1);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    m.put(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "c", "d", 1, 0L);
    m.delete(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2", 3);
    Assert.assertTrue( m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2") instanceof TombstoneValue );
    m.delete(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), 4);
    Assert.assertEquals(4, 
            ((TombstoneValue) m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "column2")).getTime()  );
    Assert.assertEquals(4, 
            ((TombstoneValue) m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "c")).getTime() );
    Assert.assertEquals(4, ((TombstoneValue)m.get(ks1.getKeyspaceMetaData().getPartitioner().partition("row1"), "k")).getTime()) ;
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
