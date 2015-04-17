/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru;

import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.metadata.XmlStorage;
import io.teknek.nibiru.plugins.CompactionManager;
import io.teknek.nibiru.plugins.HintReplayer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Configuration {
  private String dataDirectory;
  private String commitlogDirectory;
  private String metaDataStorageClass = XmlStorage.class.getName();
  private int transportPort = 7070;
  private String transportHost = "127.0.0.1";
  private String clusterMembershipClass = GossipClusterMembership.class.getName();
  private Map<String,Object> clusterMembershipProperties;
  private boolean httpDumpOnStop = false;
  private List<String> plugins = Arrays.asList(CompactionManager.class.getName(), HintReplayer.class.getName());
  private Map<String,Map<String,String>> pluginProperties = new HashMap<String,Map<String,String>>();
  
  public Configuration(){
  }
  
  public String getDataDirectory() {
    return dataDirectory;
  }
  
  public void setDataDirectory(String sstableDirectory) {
    this.dataDirectory = sstableDirectory;
  } 
  
  public String getCommitlogDirectory() {
    return commitlogDirectory;
  }

  public void setCommitlogDirectory(String commitlogDirectory) {
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

  public boolean isHttpDumpOnStop() {
    return httpDumpOnStop;
  }

  public void setHttpDumpOnStop(boolean httpDumpOnStop) {
    this.httpDumpOnStop = httpDumpOnStop;
  }

  public List<String> getPlugins() {
    return plugins;
  }

  public void setPlugins(List<String> plugins) {
    this.plugins = plugins;
  }

  public Map<String, Map<String, String>> getPluginProperties() {
    return pluginProperties;
  }

  public void setPluginProperties(Map<String, Map<String, String>> pluginProperties) {
    this.pluginProperties = pluginProperties;
  }  
  
}
