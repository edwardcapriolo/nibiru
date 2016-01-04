package io.teknek.nibiru.client;

import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.keyvalue.Set;
import io.teknek.nibiru.transport.rpc.BlockingRpc;
import io.teknek.nibiru.transport.rpc.BlockingRpcResponse;
import io.teknek.nit.NitDesc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.type.TypeReference;

public class RpcClient extends Client{

  public RpcClient(String host, int port, int connectionTimeoutMillis, int socketTimeoutMillis) {
    super(host, port, connectionTimeoutMillis, socketTimeoutMillis);
  }

   public <T> BlockingRpcResponse<T> blockingRpc(NitDesc desc, long duration, TimeUnit unit, TypeReference resultClass) throws ClientException {
    BlockingRpc m = new BlockingRpc();
    m.setNitDesc(desc);
    m.setTimeoutInMillis(unit.toMillis(duration));
    try {
      BlockingRpcResponse<T> response = (BlockingRpcResponse<T>) post(m, resultClass);
      if (response.getException() != null){
        throw new RuntimeException(response.getException());
      }
      return response;
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
}
