package io.teknek.nibiru;

import io.teknek.nibiru.metadata.XmlStorage;

import java.io.File;
import java.util.Map;

public class Configuration {
  private File dataDirectory;
  private File commitlogDirectory;
  private String metaDataStorageClass = XmlStorage.class.getName();
  private int transportPort = 7070;
  private String transportHost = "127.0.0.1";
  private String clusterMembershipClass;
  private Map<String,Object> clusterMembershipProperties;
  
  public Configuration(){
  }
  
  public File getDataDirectory() {
    return dataDirectory;
  }
  
  public void setDataDirectory(File sstableDirectory) {
    this.dataDirectory = sstableDirectory;
  }
  
  public File getCommitlogDirectory() {
    return commitlogDirectory;
  }

  public void setCommitlogDirectory(File commitlogDirectory) {
    this.commitlogDirectory = commitlogDirectory;
  }

  public String getMetaDataStorageClass() {
    return metaDataStorageClass;
  }

  public void setMetaDataStorageClass(String metaDataStorageClass) {
    this.metaDataStorageClass = metaDataStorageClass;
  }

  public int getTransportPort() {
    return transportPort;
  }

  public void setTransportPort(int transportPort) {
    this.transportPort = transportPort;
  }

  public String getTransportHost() {
    return transportHost;
  }

  public void setTransportHost(String transportHost) {
    this.transportHost = transportHost;
  }

  public String getClusterMembershipClass() {
    return clusterMembershipClass;
  }

  public void setClusterMembershipClass(String clusterMembershipClass) {
    this.clusterMembershipClass = clusterMembershipClass;
  }

  public Map<String, Object> getClusterMembershipProperties() {
    return clusterMembershipProperties;
  }

  public void setClusterMembershipProperties(Map<String, Object> clusterMembershipProperties) {
    this.clusterMembershipProperties = clusterMembershipProperties;
  }  
  
}
