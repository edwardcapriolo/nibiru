package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException{
    File tempFolder = testFolder.newFolder("sstable");
    System.out.println("Test folder: " + testFolder.getRoot());
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    Memtable m = new Memtable();
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
    SSTable s = new SSTable();
    s.flushToDisk("1", configuration, m);
    s.open("1", configuration);
    long x = System.currentTimeMillis();
    for (int i = 0 ; i < 50000 ; i++) {
      Assert.assertEquals("d", s.get("row1", "column2").getValue());
      Assert.assertEquals("e", s.get("row1", "column3").getValue());
      Assert.assertEquals("e", s.get("row2", "column1").getValue());
    }
    System.out.println((System.currentTimeMillis() - x));
  }
  
}
