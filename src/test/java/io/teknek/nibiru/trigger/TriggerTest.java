package io.teknek.nibiru.trigger;

import java.util.List;
import java.util.SortedMap;

import junit.framework.Assert;

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
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TriggerTest {

  @Rule
  public TemporaryFolder node1Folder = new TemporaryFolder();
  
  private Server server; 
  
  @Before
  public void buildServer(){
    server = TestUtil.aBasicServer(node1Folder);
  }
  
  @After
  public void closeServer(){
    server.shutdown();
  }
  
  @Test
  public void helloTrigger() throws ClientException{
    List<TriggerDefinition> defs = server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores()
            .get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().getCoordinatorTriggers();
    TriggerDefinition td = new TriggerDefinition();
    td.setTriggerClass(PrintlnCoordinatorTrigger.class.getName());
    td.setTriggerLevel(TriggerLevel.BLOCKING);
    defs.add(td);
    ColumnFamilyClient client = new ColumnFamilyClient( new Client(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort()));
    MetaDataClient meta = new MetaDataClient(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort());
    Session s = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(TestUtil.PETS_COLUMN_FAMILY).build();
    s.put("a", "b", "c", 1L);
    System.out.println( s.get("a", "b"));
    meta.shutdown();
  }
  
  
  private static final String PET_AGE_CF = "petage";
  
  @Test
  public void reverseIndexTrigger() throws ClientException{
    
    MetaDataClient meta = new MetaDataClient(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort());
    meta.createOrUpdateStore(
            TestUtil.DATA_KEYSPACE,
            PET_AGE_CF,
            new Response().withProperty(StoreMetaData.IMPLEMENTING_CLASS,
                    DefaultColumnFamily.class.getName()));
    
    //We need a way to do this from the metaclient
    TriggerDefinition td = new TriggerDefinition();
    td.setTriggerClass(PetAgeReverseTrigger.class.getName());
    td.setTriggerLevel(TriggerLevel.BLOCKING);
    List<TriggerDefinition> defs = server.getKeyspaces().get(TestUtil.DATA_KEYSPACE).getStores()
            .get(TestUtil.PETS_COLUMN_FAMILY).getStoreMetadata().getCoordinatorTriggers();
    defs.add(td);
    
    ColumnFamilyClient client = new ColumnFamilyClient( new Client(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort()));
    
    Session s = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(TestUtil.PETS_COLUMN_FAMILY).build();
    s.put("rover", "age", "5", 1L);
    s.put("sandy", "age", "3", 1L);
    s.put("spot", "age", "5", 1L);
    
    
    Session s1 = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(PET_AGE_CF).build();
    SortedMap<String,Val> res = s1.slice("5", "a", "zzzzzzzzzzzzzzzzz");
    Assert.assertEquals(2, res.size());
    Assert.assertEquals("rover", res.firstKey());
    Assert.assertEquals("spot", res.lastKey());
    
    meta.shutdown();
  }
  
  public static class PetAgeReverseTrigger implements CoordinatorTrigger {
    @Override
    public void exec(Message message, Response response, Server server) {
      String column = (String) message.getPayload().get("column");
      String value = (String) message.getPayload().get("value");
      String rowkey = (String) message.getPayload().get("rowkey");
      if ("age".equalsIgnoreCase(column)){
        Message m = new Message();
        m.setKeyspace("data");
        m.setStore(PET_AGE_CF);
        m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
        m.setPayload( new Response().withProperty("type", "put")
                .withProperty("rowkey", value)
                .withProperty("column", rowkey)
                .withProperty("value", "")
                .withProperty("time", System.currentTimeMillis())
                );
        server.getCoordinator().handle(m);
      }
    }    
  }
  
  
  
}
