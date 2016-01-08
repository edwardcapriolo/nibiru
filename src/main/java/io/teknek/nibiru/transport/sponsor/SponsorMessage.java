package io.teknek.nibiru.transport.sponsor;

import io.teknek.nibiru.transport.BaseMessage;

public class SponsorMessage extends BaseMessage{
  private String keyspace;
  private String requestId;
  private String wantedToken;
  private String transportHost;

  public SponsorMessage() {
  }

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public String getWantedToken() {
    return wantedToken;
  }

  public void setWantedToken(String wantedToken) {
    this.wantedToken = wantedToken;
  }

  public String getTransportHost() {
    return transportHost;
  }

  public void setTransportHost(String transportHost) {
    this.transportHost = transportHost;
  }
}
