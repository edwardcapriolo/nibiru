package io.teknek.nibiru.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.SortedMap;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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
    Server s =TestUtil.aBasicServer(testFolder);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "name", "bunnyjack", 1);
    s.put(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age", "6", 1);
    AtomValue x = s.get(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "jack", "age");
    Assert.assertEquals("6", ((ColumnValue) x).getValue());
    ColumnFamilyClient c = new ColumnFamilyClient(new Client("127.0.0.1", s.getConfiguration().getTransportPort(),10000,10000));
    Session session = c.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE)
      .withStore(TestUtil.PETS_COLUMN_FAMILY)
      .build();
    Assert.assertEquals("6", session.get("jack", "age").getValue());
    Session sessionTrace = c.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE)
            .withStore(TestUtil.PETS_COLUMN_FAMILY).withTracing(TraceTo.LOGGER)
            .build();
    Assert.assertEquals("6", sessionTrace.get("jack", "age").getValue());
    Assert.assertEquals("bunnyjack", session.get("jack", "name").getValue());
    session.delete("jack", "name", 2L);
    Assert.assertEquals(null, session.get("jack", "name").getValue());
    session.put("jack", "weight", "6lbds", 2L);
    Assert.assertEquals("6lbds", session.get("jack", "weight").getValue());
    session.put("jack", "height", "7in", 10L);
    Assert.assertEquals("7in", session.get("jack", "height").getValue());
    SortedMap<String,Val> slice = session.slice("jack", "a", "z");
    System.out.println(slice);
    Assert.assertEquals("6", slice.get(slice.firstKey()).getValue());
    Assert.assertEquals("7in", slice.get("height").getValue());
    Assert.assertEquals("6lbds", slice.get("weight").getValue());
    
    {
      Client cl = new Client("127.0.0.1", s.getConfiguration().getTransportPort());
      Response r = cl.post(new Message());
      Assert.assertTrue(r.containsKey("exception"));
    }
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
