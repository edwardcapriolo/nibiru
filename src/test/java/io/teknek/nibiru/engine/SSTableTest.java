package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Keyspace;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SSTableTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void aTest() throws IOException{
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createColumnFamily("abc");
    Memtable m = new Memtable(ks1.getColumnFamilies().get("abc"), new CommitLog(ks1.getColumnFamilies().get("abc")));
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
    SsTable s = new SsTable(ks1.getColumnFamilies().get("abc"));
    //s.flushToDisk("1", configuration, m);
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", ks1.getColumnFamilies().get("abc"), m);
    s.open("1", ks1.getConfiguration());
    long x = System.currentTimeMillis();
    for (int i = 0 ; i < 50000 ; i++) {
      Assert.assertEquals("d", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
      Assert.assertEquals("e", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3").getValue());
      Assert.assertEquals("e", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1").getValue());
    }
    System.out.println((System.currentTimeMillis() - x));
    
    Assert.assertEquals("row1", s.getStreamReader().getNextToken().getRowkey());
  }
  
  public static Configuration getBasicConfiguration(TemporaryFolder testFolder){
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    configuration.setCommitlogDirectory(commitlog);
    return configuration;
  }
  
  @Test
  public void aBiggerTest() throws IOException, InterruptedException{
    
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createColumnFamily("abc");
    Memtable m = new Memtable(ks1.getColumnFamilies().get("abc"), new CommitLog(ks1.getColumnFamilies().get("abc")));
    for (int i = 0; i < 10000; i++) {
      NumberFormat nf = new DecimalFormat("00000");
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition(nf.format(i)), "column2", "c", 1, 0L);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition(nf.format(i)), "column3", "c", 1, 0L);
    }
    SsTable s = new SsTable(ks1.getColumnFamilies().get("abc"));
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", ks1.getColumnFamilies().get("abc"), m);
    s.open("1", ks1.getConfiguration());
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("00001"), "column2").getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("08999"), "column2").getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("00001"), "column2").getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("08999"), "column2").getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    
    
  }
    
}
