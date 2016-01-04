package io.teknek.nibiru.transport.rpc;

import io.teknek.nibiru.transport.BaseResponse;
import io.teknek.nibiru.transport.Response;

public class BlockingRpcResponse implements BaseResponse {

  private String exception;
  private Object rpcResult;

  public  BlockingRpcResponse(){
    
  }

  public String getException() {
    return exception;
  }

  public void setException(String exception) {
    this.exception = exception;
  }

  public Object getRpcResult() {
    return rpcResult;
  }

  public void setRpcResult(Object rpcResult) {
    this.rpcResult = rpcResult;
  }
  
}
