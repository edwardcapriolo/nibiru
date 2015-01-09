package io.teknek.nibiru.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BasicTransportTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Rule
  public TemporaryFolder testFolder2 = new TemporaryFolder();

  @Test
  public void doIt() throws IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException, ClientException {
    Server s = TestUtil.aBasicServer(testFolder);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name", "bunnyjack", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age", "6", 1);
    Val x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    ColumnFamilyClient cl = new ColumnFamilyClient("127.0.0.1", s.getConfiguration().getTransportPort());
    Assert.assertEquals("6", cl.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age").getValue());
    Assert.assertEquals("bunnyjack", cl.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name").getValue());
    cl.delete(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name", 2L);
    Assert.assertEquals(null, cl.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name").getValue());
    cl.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "weight", "6lbds", 2L);
    Assert.assertEquals("6lbds", cl.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "weight").getValue());
    cl.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "height", "7in", 10L);
    Assert.assertEquals("7in", cl.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "height").getValue());
    Response r = cl.post(new Message());
    Assert.assertTrue(r.containsKey("exception"));
    s.shutdown();
  }
  
  @Test
  public void testBindHost(){
    Configuration configuration = TestUtil.aBasicConfiguration(testFolder);
    configuration.setTransportHost("127.0.0.1");
    Server s = new Server(configuration);
    s.init();
    Configuration configuration2 = TestUtil.aBasicConfiguration(testFolder2);
    configuration2.setTransportHost("127.0.0.2");
    Server s2 = new Server(configuration2);
    s2.init();
    s.shutdown();
    s2.shutdown();
  }
}
