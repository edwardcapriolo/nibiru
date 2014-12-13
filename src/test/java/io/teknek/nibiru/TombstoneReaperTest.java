package io.teknek.nibiru;

import io.teknek.nibiru.engine.Val;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class TombstoneReaperTest {
  @Test 
  public void testTombstoneGrace(){
    String keyspace = "testks";
    String columnFamily = "testcf";
    Server s = new Server();
    s.createKeyspace(keyspace); 
    s.createColumnFamily(keyspace, columnFamily);
    s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily).getColumnFamilyMetadata().setTombstoneGraceMillis(2);
    s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily).getMemtable().setTimeSource(
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
      Assert.assertEquals(results, s.slice(keyspace, columnFamily, "mykey", "a", "z"));
    }
    s.getTombstoneReaper().processColumnFamily(s.getKeyspaces().get(keyspace).getColumnFamilies().get(columnFamily), 5);
    {
      Map<String, Val> results = new HashMap<>();
      results.put("mycolumn2", new Val("abc", 1, 2, 0));
      Assert.assertEquals(results, s.slice(keyspace, columnFamily, "mykey", "a", "z"));
    }
  }

}
