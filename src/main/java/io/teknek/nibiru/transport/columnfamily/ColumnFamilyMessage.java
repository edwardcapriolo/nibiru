package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.ConsistencySupport;
import io.teknek.nibiru.transport.Routable;

public abstract class ColumnFamilyMessage extends BaseMessage implements Routable, ConsistencySupport {
  private String keyspace;
  private String store;
  private boolean reRoute;
  private Consistency consistency;
  
  public ColumnFamilyMessage(){}

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getStore() {
    return store;
  }

  public void setStore(String store) {
    this.store = store;
  }

  @Override
  public abstract String determineRoutingInformation();

  @Override
  public boolean getReRoute() {
    return reRoute;
  }

  @Override
  public void setReRoute(boolean reRoute) {
    this.reRoute = reRoute;
  }

  public Consistency getConsistency() {
    return consistency;
  }

  public void setConsistency(Consistency consistency) {
    this.consistency = consistency;
  }
  
}
