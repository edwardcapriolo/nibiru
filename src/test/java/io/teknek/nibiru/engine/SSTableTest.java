package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class SSTableTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  
  @Test
  public void aTest() throws IOException{
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createColumnFamily("abc", new ImmutableMap.Builder<String,Object>().put( ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()).build());
    Memtable m = new Memtable(ks1.getColumnFamilies().get("abc"), new CommitLog(ks1.getColumnFamilies().get("abc")));
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "c", 1, 0L);
    Assert.assertEquals("c", ((ColumnValue)m.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2")).getValue());
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2", "d", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3", "e", 2, 0L);
    m.put(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1", "e", 2, 0L);
    SsTable s = new SsTable(ks1.getColumnFamilies().get("abc"));
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", ks1.getColumnFamilies().get("abc"), m);
    s.open("1", ks1.getConfiguration());
    long x = System.currentTimeMillis();
    for (int i = 0 ; i < 50000 ; i++) {
      Assert.assertEquals("d", ((ColumnValue) s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column2")).getValue());
      Assert.assertEquals("e", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row1"), "column3")).getValue());
      Assert.assertEquals("e", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("row2"), "column1")).getValue());
    }
    System.out.println((System.currentTimeMillis() - x));
    
    Assert.assertEquals("row1", s.getStreamReader().getNextToken().getRowkey());
  }
  
  public static Configuration getBasicConfiguration(TemporaryFolder testFolder){
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration configuration = new Configuration();
    configuration.setDataDirectory(tempFolder);
    configuration.setCommitlogDirectory(commitlog);
    return configuration;
  }
  
  @Test
  public void aBiggerTest() throws IOException, InterruptedException{
    
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createColumnFamily("abc", new ImmutableMap.Builder<String,Object>().put( ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()).build());
    Memtable m = new Memtable(ks1.getColumnFamilies().get("abc"), new CommitLog(ks1.getColumnFamilies().get("abc")));
    
    for (int i = 0; i < 100000; i++) {
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
        Assert.assertEquals("c", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("00001"), "column2")).getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("08999"), "column2")).getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("00001"), "column2")).getValue());
      }
      System.out.println("index match " + (System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("c", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("08999"), "column2")).getValue());
      }
      System.out.println("far from index " +(System.currentTimeMillis() - x));
    }
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals(null, s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("wontfindthis"), "column2"));
      }
      System.out.println("non existing key " +(System.currentTimeMillis() - x));
    }
    
    
  }
    
  
  @Test
  public void optimizeWideColumnsTest() throws IOException{
    Keyspace ks1 = MemtableTest.keyspaceWithNaturalPartitioner(testFolder);
    ks1.createColumnFamily("abc", new ImmutableMap.Builder<String,Object>().put( ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName()).build());
    Memtable m = new Memtable(ks1.getColumnFamilies().get("abc"), new CommitLog(ks1.getColumnFamilies().get("abc")));
    for (int i = 0; i < 100; i++) {
      NumberFormat nf = new DecimalFormat("00000");
      for (int j = 0;j< 100; j++) {
        m.put(ks1.getKeyspaceMetadata().getPartitioner().partition(nf.format(i)), "column"+nf.format(j), nf.format(j), 1, 0L);
      }
    }
    SsTable s = new SsTable(ks1.getColumnFamilies().get("abc"));
    SSTableWriter w = new SSTableWriter();
    w.flushToDisk("1", ks1.getColumnFamilies().get("abc"), m);
    s.open("1", ks1.getConfiguration());
    {
      long x = System.currentTimeMillis();
      for (int i = 0 ; i < 50000 ; i++) {
        Assert.assertEquals("00050", ((ColumnValue)s.get(ks1.getKeyspaceMetadata().getPartitioner().partition("00001"), "column00050")).getValue());
      }
      System.out.println("Wide column " + (System.currentTimeMillis() - x));
    }
  }
  
  
}
