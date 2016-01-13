package io.teknek.nibiru.trigger;

import java.util.List;
import java.util.SortedMap;


import io.teknek.nibiru.Server;
import io.teknek.nibiru.TestUtil;
import io.teknek.nibiru.TriggerDefinition;
import io.teknek.nibiru.TriggerLevel;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamily.PutMessage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TriggerTest {

  private static final String PET_AGE_CF = "petage";
  
  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  private Server server; 
  private ColumnFamilyClient client;
  private MetaDataClient meta;
  
  @Before
  public void buildServer(){
    server = TestUtil.aBasicServer(node1Folder);
    client = new ColumnFamilyClient( new Client(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort(), 10000, 10000));
    meta = new MetaDataClient(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort());
  }
  
  @After
  public void closeServer(){
    server.shutdown();
    client.shutdown();
    meta.shutdown();
  }
  
  @Test
  public void helloTrigger() throws ClientException{
    List<TriggerDefinition> defs = server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores()
            .get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().getCoordinatorTriggers();
    TriggerDefinition td = new TriggerDefinition();
    td.setTriggerClass(PrintlnCoordinatorTrigger.class.getName());
    td.setTriggerLevel(TriggerLevel.BLOCKING);
    defs.add(td);
    Session s = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(TestUtil.PETS_COLUMN_FAMILY).build();
    s.put("a", "b", "c", 1L);
  }
  
  @Test
  public void reverseIndexTrigger() throws ClientException{
    meta.createOrUpdateStore(
            TestUtil.DATA_KEYSPACE,
            PET_AGE_CF,
            new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS,
                    DefaultColumnFamily.class.getName()), true);
    
    //We need a way to do this from the metaclient
    TriggerDefinition td = new TriggerDefinition();
    td.setTriggerClass(PetAgeReverseTrigger.class.getName());
    td.setTriggerLevel(TriggerLevel.BLOCKING);
    List<TriggerDefinition> defs = server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores()
            .get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().getCoordinatorTriggers();
    defs.add(td); 
    Session s = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(TestUtil.PETS_COLUMN_FAMILY).build();
    s.put("rover", "age", "5", 1L);
    s.put("sandy", "age", "3", 1L);
    s.put("spot", "age", "5", 1L);
    
    Session s1 = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(PET_AGE_CF).build();
    SortedMap<String,Val> res = s1.slice("5", "a", "zzzzzzzzzzzzzzzzz");
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("rover", res.firstKey());
    Assert.assertEquals("spot", res.lastKey());
  }
  
  public static class PetAgeReverseTrigger implements CoordinatorTrigger {
    @Override
    public void exec(BaseMessage message, Response response, Server server) {
      if (!(message instanceof PutMessage))
        return;
      PutMessage p = (PutMessage) message;
      if ("age".equalsIgnoreCase(p.getColumn())){
        PutMessage m = new PutMessage();
        m.setKeyspace("data");
        m.setStore(PET_AGE_CF);
        m.setRow(p.getValue());
        m.setColumn(p.getRow());
        m.setValue("");
        m.setVersion(System.currentTimeMillis());
        server.getCoordinator().handle(m);
      }
    }    
  }
  
}
