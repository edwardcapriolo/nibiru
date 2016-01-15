package io.teknek.nibiru;

import java.util.Queue;

import org.junit.After;

public class ServerShutdown {

  private Queue<Server> servers = new java.util.concurrent.ConcurrentLinkedQueue<>();
  
  public Server registerServer(Server s){
    servers.add(s);
    return s;
  }
  
  @After
  public void after(){
    for (Server s: servers){
      if (s != null){
        try { 
          s.shutdown(); 
        } catch (Exception e){
          System.err.println(e);
        }
      }
      servers.remove(s);
    }
  }
  
}
