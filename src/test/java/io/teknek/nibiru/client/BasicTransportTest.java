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

import com.google.common.collect.ImmutableMap;

public class BasicTransportTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void doIt() throws IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException {
    String ks = "data";
    String cf = "pets";
    Configuration configuration = ServerTest.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(ks, null);
    s.getMetaDataManager().createColumnFamily(ks, cf, null);
    s.getKeyspaces().get(ks).getColumnFamilies().get(cf).getColumnFamilyMetadata()
            .setFlushNumberOfRowKeys(2);
    s.put(ks, cf, "jack", "name", "bunnyjack", 1);
    s.put(ks, cf, "jack", "age", "6", 1);
    Val x = s.get(ks, cf, "jack", "age");
    Assert.assertEquals("6", x.getValue());

    Client cl = new Client();
    Message m = new Message();
    m.setKeyspace(ks);
    m.setColumnFamily(cf);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "get")
            .put("rowkey", "jack")
            .put("column", "age").build();
    m.setPayload(payload);
    ObjectMapper om = new ObjectMapper();
    Response response = cl.post("http://127.0.0.1:" + configuration.getTransportPort(), m);
    Assert.assertEquals("6", om.convertValue(response.get("payload"), Val.class).getValue());
  }
}
