package io.teknek.nibiru;

import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

@Ignore
public class AbstractTestServer {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  protected Server server;
  private ColumnFamilyClient client;
  private MetaDataClient meta;
  
  @Before
  public void before(){
    server = TestUtil.aBasicServer(testFolder);
    client = new ColumnFamilyClient( new Client(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort(), 10000, 10000));
    meta = new MetaDataClient(server.getConfiguration().getTransportHost(), 
            server.getConfiguration().getTransportPort());
  }
  
  @After
  public void after(){
    server.shutdown();
    client.shutdown();
    meta.shutdown();
  }
  
}
