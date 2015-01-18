package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

//potential callable
public abstract class LocalAction {

  protected Message message;
  protected Keyspace keyspace; 
  protected ColumnFamily columnFamily;
  
  public LocalAction(Message message, Keyspace ks, ColumnFamily cf){
    this.message = message;
    this.keyspace = ks;
    this.columnFamily = cf;
  }
  
  public abstract Response handleReqest();
}
