import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

public class MemtableTest {
  
  private Keyspace keyspaceWithNaturalPartitioner(){
    Keyspace ks1 = new Keyspace();
    ks1.setKeyspaceMetadata(new KeyspaceMetadata("testks"));
    ks1.getKeyspaceMetadata().setPartitioner(new NaturalPartitioner());
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
