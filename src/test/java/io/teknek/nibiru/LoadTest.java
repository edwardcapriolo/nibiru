package io.teknek.nibiru;

import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.Session;

import java.util.concurrent.Callable;

import org.junit.Test;

public class LoadTest extends AbstractTestServer {

  @Test
  public void loadMeUp(){
    
  }
  
  public static class Blast implements Callable{

    private int start;
    private int end;
    private ColumnFamilyClient client;
    
    public Blast(int start, int end, ColumnFamilyClient client){
      this.start = start;
      this.end = end;
      this.client = client;
    }
    
    @Override
    public Object call() throws Exception {
      Session s = client.createBuilder().withKeyspace(TestUtil.DATA_KEYSPACE).withStore(TestUtil.PETS_COLUMN_FAMILY).build();
      for (int i = start; i < end; i++){
        s.put(i+"", "1", "", 1);
      }
      return null;
    }
    
  }
}
