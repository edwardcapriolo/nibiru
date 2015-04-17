package io.teknek.nibiru.trigger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
  
  public Response executeTriggers(final Message message, final Response response, Keyspace keyspace, 
          Store store, long timeoutInMs, long requestStart){
    long now = System.currentTimeMillis();
    for (TriggerDefinition d : store.getStoreMetadata().getCoordinatorTriggers()){
      if (d.getTriggerLevel() == TriggerLevel.BLOCKING){
        long remaining =   (requestStart + timeoutInMs) - now;
        if (remaining > 0){
          final CoordinatorTrigger ct = getReusableTrigger(d);
          Callable<Boolean> c = new Callable<Boolean>(){
            public Boolean call() throws Exception {
              ct.exec(message, response, server);
              return Boolean.TRUE;
            }
          };
          Future<Boolean> f = null;
          try {
            f = executor.submit(c);
            Boolean b = f.get(remaining, TimeUnit.MILLISECONDS);
            if (!b.equals(Boolean.TRUE)){
              return new Response().withProperty("exception", "trigger returned  false");
            }
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            f.cancel(true);
            return new Response().withProperty("exception", "trigger exception " + e.getMessage());
            
          }
        }
      }
    }
    return response;
  }

  public void init() {
    executor = Executors.newFixedThreadPool(1024);
  }

  public void shutdown() {
    executor.shutdown();
  }

}
