package io.teknek.nibiru.coordinator;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.rpc.BlockingRpc;
import io.teknek.nibiru.transport.rpc.RpcMessage;
import io.teknek.nit.NitException;
import io.teknek.nit.NitFactory;

public class RpcCoordinator {

  private ExecutorService executor;
  private Configuration configuration;
  private NitFactory n ;
  
  public RpcCoordinator(Configuration configuration){
    this.configuration = configuration;
  }
  
  public void init(){ 
    executor = Executors.newCachedThreadPool();
    n = new NitFactory();
  }
  
  public void shutdown(){ 
    executor.shutdown();
    try {
      executor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  public Response processMessage(final RpcMessage message){
    if (message instanceof BlockingRpc){
      BlockingRpc rpc = (BlockingRpc) message;
      try {
        Object o = n.construct(((BlockingRpc) message).getNitDesc());
        if (o instanceof Callable){
          Future f = executor.submit((Callable) o);
          try {
            Object result = f.get(rpc.getTimeoutInMillis(), TimeUnit.MILLISECONDS);
            return new Response().withProperty("payload", result);
          } catch (InterruptedException | ExecutionException | TimeoutException e) {
            f.cancel(true);
          }
        }
        return new Response();
      } catch (NitException e) {
        return new Response();
      }
    } else throw new RuntimeException("Can not process "+ message);
  }
}
