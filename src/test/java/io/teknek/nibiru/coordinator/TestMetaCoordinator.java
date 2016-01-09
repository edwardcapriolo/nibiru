package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.ContactInformation;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.transport.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMetaCoordinator {

  @Rule
  public TemporaryFolder testFolder3 = new TemporaryFolder();
  
  @Test
  public void listStores() throws ClientException {
    Server s = TestUtil.aBasicServer(testFolder3, 7073);
    MetaDataClient c = new MetaDataClient(s.getConfiguration().getTransportHost(), 
            s.getConfiguration().getTransportPort());
    Assert.assertEquals(Arrays.asList(TestUtil.PETS_COLUMN_FAMILY, TestUtil.BOOKS_KEY_VALUE), 
            c.listStores(TestUtil.DATA_KEYSPACE)); 
    Assert.assertEquals(new HashMap(), c.getKeyspaceMetadata(TestUtil.DATA_KEYSPACE));
    
    Assert.assertEquals(new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS, 
            DefaultColumnFamily.class.getName()).withProperty(StoreMetaData.COORDINATOR_TRIGGERS, new ArrayList()), 
            c.getStoreMetadata(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY));
    
    Response expected = new Response().withProperty("a", "b");
    c.createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, expected, true);
    Assert.assertEquals(expected, c.getKeyspaceMetadata(TestUtil.DATA_KEYSPACE));
    Assert.assertEquals(expected, s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getKeyspaceMetaData().getProperties());
    changeAndAssert(c, s);
    assertContactInformation(c, s);
    c.shutdown();
    s.shutdown();
  }
  
  private void changeAndAssert(MetaDataClient c, Server s) throws ClientException{
    Response change = new Response().withProperty("d", "e");
    c.createOrUpdateKeyspace(TestUtil.DATA_KEYSPACE, change, true);
    Assert.assertEquals(change, c.getKeyspaceMetadata(TestUtil.DATA_KEYSPACE));
    Assert.assertEquals(change, s.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getKeyspaceMetaData().getProperties());
  }
  
  private void assertContactInformation(MetaDataClient c, Server s) throws ClientException{
    List<ContactInformation> d = c.getLocationForRowKey(TestUtil.DATA_KEYSPACE, TestUtil.PETS_COLUMN_FAMILY, "abc");
    Assert.assertEquals(d.get(0).getDestination().getDestinationId(), s.getServerId().getU().toString());
    Assert.assertEquals(d.get(0).getTransportHost(), s.getConfiguration().getTransportHost());
  }
}
