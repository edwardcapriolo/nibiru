package io.teknek.nibiru.engine;
import io.teknek.nibiru.engine.Keyspace;
import io.teknek.nibiru.engine.Memtable;
import io.teknek.nibiru.engine.Val;
import io.teknek.nibiru.metadata.KeyspaceMetadata;
import io.teknek.nibiru.partitioner.Md5Partitioner;
import io.teknek.nibiru.partitioner.NaturalPartitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

public class MemtableTest {
  
  public Keyspace keyspaceWithNaturalPartitioner(){
    Keyspace ks1 = new Keyspace();
    ks1.setKeyspaceMetadata(new KeyspaceMetadata("testks"));
    ks1.getKeyspaceMetadata().setPartitioner(new NaturalPartitioner());
    return ks1;
  }
  
  private Keyspace keyspaceWithMd5Partitioner(){
    Keyspace ks1 = new Keyspace();
    ks1.setKeyspaceMetadata(new KeyspaceMetadata("testks"));
    ks1.getKeyspaceMetadata().setPartitioner(new Md5Partitioner());
    return ks1;
  }
  
  @Test
  public void test(){
    Memtable m = new Memtable();
    Keyspace ks1 = keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    Assert.assertEquals("d", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "c", "d", 1, 0L);
    Map expected  = new TreeMap();
    expected.put("column2", new Val("d",2, System.currentTimeMillis(), 0));
    expected.put("c", new Val("d",1, System.currentTimeMillis(), 0));
    Assert.assertEquals(expected, m.slice(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "a", "z"));
    
  }
  
  @Test
  public void testDeleting(){
    Memtable m = new Memtable();
    Keyspace ks1 = keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "c", "d", 1, 0L);
    m.delete(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", 3);
    Assert.assertEquals(new Val(null,3, System.currentTimeMillis(), 0), 
            m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2"));
  }
  
  @Test
  public void testDeletingM5d(){
    Memtable m = new Memtable();
    Keyspace ks1 = this.keyspaceWithMd5Partitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "c", "d", 1, 0L);
    m.delete(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", 3);
    Assert.assertEquals(new Val(null,3, System.currentTimeMillis(), 0), 
            m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2"));
  }
  
  
  @Test
  public void testRowDelete(){
    Memtable m = new Memtable();
    Keyspace ks1 = keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0l);
    m.delete(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), 2);
    Assert.assertEquals(null, m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2"));
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 3, 0L);
    Assert.assertEquals(new Val("c",3, System.currentTimeMillis(), 0), 
            m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2"));
  }
  
  @Test
  public void aSliceWithTomb(){
    Memtable m = new Memtable();
    Keyspace ks1 = keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1L , 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "d", 4L, 0L);
    m.delete(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), 3);
    Map result = new HashMap();
    result.put("column3", new Val("d", 4, System.currentTimeMillis(), 0));
    Assert.assertEquals( result, m.slice(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "a", "z"));
  }
}