import junit.framework.Assert;

import org.junit.Test;


public class ServerTest {

  @Test
  public void server(){
    String keyspace = "testks";
    String columnFamily = "testcf";
    Server s = new Server();
    s.createKeyspace(keyspace); 
    s.createColumnFamily(keyspace, columnFamily);
    s.set(keyspace, columnFamily, "mykey", "mycolumn", "abc", 1);
    Assert.assertEquals("abc", s.get(keyspace, columnFamily, "mykey", "mycolumn").getValue());
  }
}
