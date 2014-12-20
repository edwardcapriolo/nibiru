package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCompactionManager {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void test() throws IOException{
    File tempFolder = testFolder.newFolder("sstable");
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    ColumnFamily cf = new ColumnFamily(new Keyspace(configuration));
    cf.setColumnFamilyMetadata(new ColumnFamilyMetadata());
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner();
    SsTable s = new SsTable(cf);
    {
      Memtable m = new Memtable(cf);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
      SSTableWriter w = new SSTableWriter();
      w.flushToDisk("1", configuration, m);
      s.open("1", configuration);
    }
    SsTable s2 = new SsTable(cf);
    Memtable m2 = new Memtable(cf);
    {
      m2.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column1", "c", 1, 0L);
      m2.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "f", 3, 0L);
      SSTableWriter w2 = new SSTableWriter();
      w2.flushToDisk("2", configuration, m2);
      s2.open("2", configuration);
    }
    CompactionManager cm = new CompactionManager(cf);
    cm.compact(new SsTable [] { s,s2 });
  }
}
