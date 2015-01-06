package io.teknek.nibiru.client;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import io.teknek.nibiru.ColumnFamilyPersonality;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.ServerTest;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BasicTransportTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void doIt() throws IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException, ClientException {
    String ks = "data";
    String cf = "pets";
    Configuration configuration = ServerTest.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    s.put(ks, cf, "jack", "name", "bunnyjack", 1);
    s.put(ks, cf, "jack", "age", "6", 1);
    Val x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());
    ColumnFamilyClient cl = new ColumnFamilyClient("127.0.0.1", configuration.getTransportPort());
    Assert.assertEquals("6", cl.get(ks, cf, "jack", "age").getValue());
    Assert.assertEquals("bunnyjack", cl.get(ks, cf, "jack", "name").getValue());
    s.shutdown();
  }
}
