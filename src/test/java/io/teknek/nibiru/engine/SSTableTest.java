package io.teknek.nibiru.engine;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class SSTableTest {

  @Test
  public void aTest() throws IOException{
    Memtable m = new Memtable();
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    SSTable s = new SSTable();
    s.flushToDisk(m);
    
    s.open(null, null);
    s.get("a", "b");
  }
}
