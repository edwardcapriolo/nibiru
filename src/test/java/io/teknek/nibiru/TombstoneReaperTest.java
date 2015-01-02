package io.teknek.nibiru;


import java.io.File;
import java.util.HashMap;
import java.util.Map;
import io.teknek.nibiru.engine.DefaultColumnFamily;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TombstoneReaperTest {
  
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test 
  public void testTombstoneGrace(){
    String keyspace = "testks";
    String columnFamily = "testcf";
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration c = new Configuration();
    c.setDataDirectory(tempFolder);
    Server s = new Server(c);
    s.init();
    s.createKeyspace(keyspace); 
    s.createColumnFamily(keyspace, columnFamily);
    s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily).getColumnFamilyMetadata().setTombstoneGraceMillis(2);
    ((DefaultColumnFamily) s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily)).getMemtable().setTimeSource(
            new TimeSource(){
              public long getTimeInMillis() {
                return 2;
              }}
            );
    s.put(keyspace, columnFamily, "mykey", "mycolumn", "abc", 1L);
    s.put(keyspace, columnFamily, "mykey", "mycolumn2", "abc", 1L);
    s.delete(keyspace, columnFamily, "mykey", "mycolumn", 3);
    s.getTombstoneReaper().processColumnFamily(s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily), 3);
    {
      Map<String, Val> results = new HashMap<>();
      results.put("mycolumn", new Val(null, 3, 2, 0));
      results.put("mycolumn2", new Val("abc", 1, 2, 0));
      //Assert.assertEquals(results, s.slice(keyspace, columnFamily, "mykey", "a", "z"));
    }
    s.getTombstoneReaper().processColumnFamily(s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily), 5);
    {
      Map<String, Val> results = new HashMap<>();
      results.put("mycolumn2", new Val("abc", 1, 2, 0));
      //Assert.assertEquals(results, s.slice(keyspace, columnFamily, "mykey", "a", "z"));
    }
  }

}
