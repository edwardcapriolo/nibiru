package io.teknek.nibiru.keyvalue;

import io.teknek.nibiru.BasicAbstractServerTest;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.KeyValueClient;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.junit.Assert;
import org.junit.Test;

public class KeyValueTests extends BasicAbstractServerTest{

  @Test
  public void doIt() throws IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException, ClientException {
    KeyValueClient k = new KeyValueClient("127.0.0.1", server.getConfiguration().getTransportPort());
    k.put(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "programming hive", "ecapriolo");
    k.put(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "high performance cassandra", "ecapriolo");
    Assert.assertEquals("ecapriolo", k.get(TestUtil.DATA_KEYSPACE, TestUtil.BOOKS_KEY_VALUE, "programming hive"));
  }
}

