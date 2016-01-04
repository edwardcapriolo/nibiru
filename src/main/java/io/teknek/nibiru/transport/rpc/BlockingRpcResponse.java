package io.teknek.nibiru.transport.rpc;

import io.teknek.nibiru.transport.BaseResponse;

public class BlockingRpcResponse<T> implements BaseResponse {

  private String exception;
  private T rpcResult;

  public  BlockingRpcResponse(){
    
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  public T getRpcResult() {
    return rpcResult;
  }

  public void setRpcResult(T rpcResult) {
    this.rpcResult = rpcResult;
  }
  
}
