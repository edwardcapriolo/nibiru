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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((host == null) ? 0 : host.hashCode());
    result = prime * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ClusterMember other = (ClusterMember) obj;
    if (host == null) {
      if (other.host != null)
        return false;
    } else if (!host.equals(other.host))
      return false;
    if (port != other.port)
      return false;
    return true;
  }
  
}
