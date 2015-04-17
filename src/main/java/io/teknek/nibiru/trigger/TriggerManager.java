package io.teknek.nibiru.trigger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.TriggerDefinition;
import io.teknek.nibiru.TriggerLevel;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class TriggerManager {
  
  private ExecutorService executor;
  private final Server server;
  
  public TriggerManager(Server server){
    this.server = server;
  }

  public CoordinatorTrigger getReusableTrigger(TriggerDefinition d){
    try {
      CoordinatorTrigger ct = (CoordinatorTrigger) Class.forName(d.getTriggerClass()).newInstance();
      return ct;
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void executeTriggers(Message message, Response response, Keyspace keyspace, Store store){
    System.out.println(store.getStoreMetadata().getCoordinatorTriggers());
    for (TriggerDefinition d : store.getStoreMetadata().getCoordinatorTriggers()){
      if (d.getTriggerLevel() == TriggerLevel.BLOCKING){
        CoordinatorTrigger ct = getReusableTrigger(d);
        ct.exec(message, response, server);
      }
    }
  }

  public void init() {
    executor = Executors.newFixedThreadPool(1024);
  }

  public void shutdown() {
    executor.shutdown();
  }

}
