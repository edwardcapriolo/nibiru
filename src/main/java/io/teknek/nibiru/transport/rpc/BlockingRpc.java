package io.teknek.nibiru.transport.rpc;

import io.teknek.nit.NitDesc;

public class BlockingRpc extends RpcMessage {
  private NitDesc nitDesc;
  private long timeoutInMillis;
  
  public BlockingRpc(){
    
  }

  public NitDesc getNitDesc() {
    return nitDesc;
  }

  public void setNitDesc(NitDesc nitDesc) {
    this.nitDesc = nitDesc;
  }

  public long getTimeoutInMillis() {
    return timeoutInMillis;
  }

  public void setTimeoutInMillis(long timeoutInMillis) {
    this.timeoutInMillis = timeoutInMillis;
  }
  
}
