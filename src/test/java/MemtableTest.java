import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Test;

public class MemtableTest {
  @Test
  public void test(){
    Memtable m = new Memtable();
    m.put("row1", "column2", "c", 1);
    Assert.assertEquals("c", m.get("row1", "column2").getValue());
    m.put("row1", "column2", "d", 2);
    Assert.assertEquals("d", m.get("row1", "column2").getValue());
    m.put("row1", "c", "d", 1);
    Map expected  = new TreeMap();
    expected.put("column2", new Val("d",2));
    expected.put("c", new Val("d",1));
    Assert.assertEquals(expected, m.slice("row1", "a", "z"));
    
  }
  
  @Test
  public void testDeleting(){
    Memtable m = new Memtable();
    m.put("row1", "column2", "c", 1);
    m.put("row1", "c", "d", 1);
    m.delete("row1", "column2", 3);
    Assert.assertEquals(new Val(null,3), m.get("row1", "column2"));
  }
  
  @Test
  public void testRowDelete(){
    Memtable m = new Memtable();
    m.put("row1", "column2", "c", 1);
    m.delete("row1", 2);
    Assert.assertEquals(null, m.get("row1", "column2"));
    m.put("row1", "column2", "c", 3);
    Assert.assertEquals(new Val("c",3), m.get("row1", "column2"));
  }
  
  @Test
  public void aSliceWithTomb(){
    Memtable m = new Memtable();
    m.put("row1", "column2", "c", 1);
    m.put("row1", "column3", "d", 4);
    m.delete("row1", 3);
    Map result = new HashMap();
    result.put("column3", new Val("d", 4));
    Assert.assertEquals( result, m.slice("row1", "a", "z"));
  }
}
