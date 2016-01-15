package io.teknek.nibiru.rpc;
import java.util.concurrent.TimeUnit;

import io.teknek.nibiru.BasicAbstractServerTest;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.RpcClient;
import io.teknek.nibiru.coordinator.RpcCoordinator;
import io.teknek.nibiru.transport.rpc.BlockingRpc;
import io.teknek.nibiru.transport.rpc.BlockingRpcResponse;
import io.teknek.nit.NitDesc;
import io.teknek.nit.NitDesc.NitSpec;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Assert;
import org.junit.Test;

public class TestRpcCoordinator extends BasicAbstractServerTest {
  
  public static final String SIMPLE_CALLABLE =  "import java.util.concurrent.Callable\n" +
          "public class A implements Callable {\n" +
          "public Object call(){\n" +
          "  return 5\n" +  
          "}\n" +
     "}\n";
  

  public NitDesc simpleCallable(){
    NitDesc d = new NitDesc();
    d.setSpec(NitSpec.GROOVY_CLASS_LOADER);
    d.setScript(SIMPLE_CALLABLE);
    return d;
  }
  
  public static final String RETURN_OBJECT =  "import java.util.concurrent.Callable\n" +
          "import io.teknek.nibiru.rpc.*\n" +
          "public class B implements Callable {\n" +
          "public Object call(){\n" +
          "  SomeWhackyType v = new SomeWhackyType()\n"+
          "  v.setY('yo')\n"+
          "  return v\n" +  
          "}\n" +
     "}\n";
  
  public NitDesc complexReturnCallable(){
    NitDesc d = new NitDesc();
    d.setSpec(NitSpec.GROOVY_CLASS_LOADER);
    d.setScript(RETURN_OBJECT);
    return d;
  }
  
  @Test
  public void test(){
    RpcCoordinator coordinator = new RpcCoordinator(new Configuration());
    coordinator.init();
    BlockingRpc rpc = new BlockingRpc();
    rpc.setTimeoutInMillis(10000);
    rpc.setNitDesc(simpleCallable());
    @SuppressWarnings("unchecked")
    BlockingRpcResponse<Integer> r = (BlockingRpcResponse<Integer>) coordinator.processMessage(rpc);
    Assert.assertEquals(new Integer(5), r.getRpcResult());
    coordinator.shutdown();
  }
  
  @Test
  public void endToEndTest() throws ClientException{
    RpcClient rpcClient = new RpcClient("127.0.0.1", server.getConfiguration().getTransportPort(), 10000, 10000);
    TypeReference<BlockingRpcResponse<Integer>> t = new TypeReference<BlockingRpcResponse<Integer>> (){};
    BlockingRpcResponse<Integer> resp = rpcClient.blockingRpc(simpleCallable(), 10000, TimeUnit.MILLISECONDS, t);
    Assert.assertEquals(new Integer(5), resp.getRpcResult());
  }
  
  @Test
  public void complexInReturn() throws ClientException{
    RpcClient rpcClient = new RpcClient("127.0.0.1", server.getConfiguration().getTransportPort(), 10000, 10000);
    TypeReference<BlockingRpcResponse<SomeWhackyType>> t = new TypeReference<BlockingRpcResponse<SomeWhackyType>> (){};
    BlockingRpcResponse<SomeWhackyType> resp = rpcClient.blockingRpc(complexReturnCallable(), 10000, TimeUnit.MILLISECONDS, t);
    ObjectMapper om = new ObjectMapper();
    Assert.assertEquals( om.convertValue( resp.getRpcResult(), SomeWhackyType.class).getY(), "yo");
    Assert.assertEquals( resp.getRpcResult().getY(), "yo");
  }
  
}
