package io.teknek.nibiru.cluster;

public class ClusterMember {

  private String host;
  private int port;
  private long heatbeat;
  private String id;
  
  public ClusterMember(){}

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public long getHeatbeat() {
    return heatbeat;
  }

  public void setHeatbeat(long heatbeat) {
    this.heatbeat = heatbeat;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "ClusterMember [host=" + host + ", port=" + port + ", heatbeat=" + heatbeat + ", id="
            + id + "]";
  }
  
}
