package io.teknek.nibiru.client;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.TraceTo;

import java.util.Map;

public class SessionBuilder {
  private final ColumnFamilyClient client;
  private String keyspace;
  private String store;
  private Consistency writeConsistency;
  private Consistency readConsistency;
  private long timeoutMillis;
  private TraceTo traceTo;
  
  public SessionBuilder(ColumnFamilyClient client){
    this.client = client;
    //writeConsistency = new Consistency().withLevel(ConsistencyLevel.N).withParamater("must-ack", 1);
    writeConsistency = new Consistency().withLevel(ConsistencyLevel.IMPLIED);
    readConsistency = new Consistency().withLevel(ConsistencyLevel.IMPLIED);
    timeoutMillis = 10000;
  }
  
  public SessionBuilder withKeyspace(String keyspace){
    this.keyspace = keyspace;
    return this;
  }
  
  public SessionBuilder withStore(String columnFamily){
    this.store = columnFamily;
    return this;
  }
    
  public SessionBuilder withWriteConsistency(ConsistencyLevel level, Map<String,Object> parameters){
    writeConsistency = new Consistency().withLevel(level).withParameters(parameters);
    return this;
  }
  
  public SessionBuilder withReadConsistency(ConsistencyLevel level, Map<String,Object> parameters){
    readConsistency = new Consistency().withLevel(level).withParameters(parameters);
    return this;
  }
  
  public SessionBuilder withTracing(TraceTo t){
    this.traceTo = t;
    return this;
  }
  
  public Session build(){
    return new Session(client, keyspace, store, writeConsistency, readConsistency, timeoutMillis, traceTo);
  }
}