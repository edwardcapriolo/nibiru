package io.teknek.nibiru.transport.keyvalue;

import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Routable;

public abstract class KeyValueMessage extends BaseMessage implements Routable{

  private String keyspace;
  private String store;
  
  protected boolean reRoute;
  
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
  
}
