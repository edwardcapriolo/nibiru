package io.teknek.nibiru;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

@Ignore
public class BasicAbstractServerTest {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  protected Server server;
  
  @Before
  public void before(){
    server = TestUtil.aBasicServer(testFolder);
  }
  
  @After
  public void after(){
    server.shutdown();
  }
}
