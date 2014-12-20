package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.metadata.ColumnFamilyMetadata;

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
    File tempFolder = testFolder.newFolder("sstable");
    System.out.println("Test folder: " + testFolder.getRoot());
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    ColumnFamily cf = new ColumnFamily(new Keyspace(configuration));
    cf.setColumnFamilyMetadata(new ColumnFamilyMetadata());
    Memtable m = new Memtable(cf);
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner();
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2").getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
    SsTable s = new SsTable(cf);
    //s.flushToDisk("1", configuration, m);
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", configuration, m);
    s.open("1", configuration);
    long x = System.currentTimeMillis();
    for (int i = 0 ; i < 50000 ; i++) {
      Assert.assertEquals("d", s.get("row1", "column2").getValue());
      Assert.assertEquals("e", s.get("row1", "column3").getValue());
      Assert.assertEquals("e", s.get("row2", "column1").getValue());
    }
    System.out.println((System.currentTimeMillis() - x));
    
    Assert.assertEquals("row1", s.getStreamReader().getNextToken().getRowkey());
  }
  
  
  @Test
  public void aBiggerTest() throws IOException, InterruptedException{
    File tempFolder = testFolder.newFolder("sstable");
    System.out.println("Test folder: " + testFolder.getRoot());
    Configuration configuration = new Configuration();
    configuration.setSstableDirectory(tempFolder);
    Memtable m = new Memtable(new ColumnFamily(new Keyspace(configuration)));
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner();
    NumberFormat nf = new DecimalFormat("00000");
    for (int i = 0; i < 10000; i++) {
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition(nf.format(i)), "column2", "c", 1, 0L);
      m.put(ks1.getKeyspaceMetadata().getPartitioner().partition(nf.format(i)), "column3", "c", 1, 0L);
    }
    ColumnFamily cf = new ColumnFamily(new Keyspace(configuration));
    cf.setColumnFamilyMetadata(new ColumnFamilyMetadata());
    SsTable s = new SsTable(cf);
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", configuration, m);
    s.open("1", configuration);
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get("00001", "column2").getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get("08999", "column2").getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get("00001", "column2").getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", s.get("08999", "column2").getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    
    
  }
    
}
