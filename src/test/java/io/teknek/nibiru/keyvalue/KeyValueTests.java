package io.teknek.nibiru.keyvalue;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.KeyValueClient;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KeyValueTests {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void doIt() throws IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException, ClientException {
    Server s = TestUtil.aBasicServer(testFolder);
    KeyValueClient k = new KeyValueClient("127.0.0.1", s.getConfiguration().getTransportPort());
    k.put(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "programming hive", "ecapriolo");
    k.put(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "high performance cassandra", "ecapriolo");
    Assert.assertEquals("ecapriolo", k.get(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "programming hive"));
    s.shutdown();
  }
}

