package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;

import java.util.Arrays;
import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMetaCoordinator {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void listStores() throws ClientException {
    Server s = TestUtil.aBasicServer(testFolder);
    MetaDataClient c = new MetaDataClient(s.getConfiguration().getTransportHost(), s.getConfiguration().getTransportPort());
    Assert.assertEquals(Arrays.asList(TestUtil.PETS_COLUMN_FAMILY, TestUtil.BOOKS_KEY_VALUE), c.listStores(TestUtil.DATA_KEYSPACE));
    c.shutdown();
    s.shutdown();
  }
}
