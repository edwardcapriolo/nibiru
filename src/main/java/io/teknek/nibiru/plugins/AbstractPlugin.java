package io.teknek.nibiru.plugins;

import io.teknek.nibiru.Server;

public abstract class AbstractPlugin {

  protected Server server;

  public AbstractPlugin(Server server) {
    this.server = server;
  }
  
  /** name of the plugin to be used in hashmap */
  public abstract String getName();
  
  public abstract void init();
  
  public abstract void shutdown();
}
