package io.teknek.nibiru.client;

import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.keyvalue.Set;
import io.teknek.nibiru.transport.rpc.BlockingRpc;
import io.teknek.nibiru.transport.rpc.BlockingRpcResponse;
import io.teknek.nit.NitDesc;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RpcClient extends Client{

  public RpcClient(String host, int port, int connectionTimeoutMillis, int socketTimeoutMillis) {
    super(host, port, connectionTimeoutMillis, socketTimeoutMillis);
  }

  public BlockingRpcResponse blockingRpc(NitDesc desc, long duration, TimeUnit unit) throws ClientException {
    BlockingRpc m = new BlockingRpc();
    m.setNitDesc(desc);
    m.setTimeoutInMillis(unit.toMillis(duration));
    try {
      BlockingRpcResponse response = (BlockingRpcResponse) post(m, BlockingRpcResponse.class);
      if (response.getException() != null){
        throw new RuntimeException(response.getException());
      }
      return response;
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
}
