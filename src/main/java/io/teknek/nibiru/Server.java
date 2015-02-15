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

import io.teknek.nibiru.cluster.ClusterMembership;
import io.teknek.nibiru.coordinator.Coordinator;
import io.teknek.nibiru.engine.CompactionManager;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.HttpJsonTransport;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Server {
  
  private final ConcurrentMap<String,Keyspace> keyspaces;
  private final Configuration configuration;
  private final MetaDataManager metaDataManager;
  private final HttpJsonTransport transport;
  private final Coordinator coordinator;
  private final ServerId serverId;
  private final ClusterMembership clusterMembership;
    
  private CompactionManager compactionManager;
  private Thread compactionRunnable;
  
  public Server(Configuration configuration){
    this.configuration = configuration;
    keyspaces = new ConcurrentHashMap<String,Keyspace>();
    compactionManager = new CompactionManager(this);
    metaDataManager = new MetaDataManager(configuration, this);
    serverId = new ServerId(configuration);
    clusterMembership = ClusterMembership.createFrom(configuration, serverId);
    coordinator = new Coordinator(this);
    transport = new HttpJsonTransport(configuration, coordinator);
  }
  
  public void init(){
    serverId.init();
    metaDataManager.init();
    compactionRunnable = new Thread(compactionManager);
    compactionRunnable.start();
    clusterMembership.init();
    coordinator.init();
    transport.init();
  }
 
  public void shutdown() {
    compactionManager.setGoOn(false);
    transport.shutdown();
    coordinator.shutdown();
    clusterMembership.shutdown();
    for (Map.Entry<String, Keyspace> entry : keyspaces.entrySet()){
      for (Map.Entry<String, ColumnFamily> columnFamilyEntry : entry.getValue().getColumnFamilies().entrySet()){
        try {
          columnFamilyEntry.getValue().shutdown();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
    
  public void put(String keyspace, String columnFamily, String rowkey, String column, String value, long time){
    Keyspace ks = keyspaces.get(keyspace);
    ColumnFamily cf = ks.getColumnFamilies().get(columnFamily);
    if (cf instanceof ColumnFamilyPersonality){
      ((ColumnFamilyPersonality) cf).put(rowkey, column, value, time, 0L);
    } else {
      throw new RuntimeException("Does not support this personality");
    }
  }
  
  public void put(String keyspace, String columnFamily, String rowkey, String column, String value, long time, long ttl){
    Keyspace ks = keyspaces.get(keyspace);
    ColumnFamily cf = ks.getColumnFamilies().get(columnFamily);
    if (cf instanceof ColumnFamilyPersonality){
      ((ColumnFamilyPersonality) cf).put(rowkey, column, value, time);
    } else {
      throw new RuntimeException("Does not support this personality");
    }
  }
  
  public AtomValue get(String keyspace, String columnFamily, String rowkey, String column){
    Keyspace ks = keyspaces.get(keyspace);
    if (ks == null){
      throw new RuntimeException(keyspace + " is not found");
    }
    ColumnFamily cf = ks.getColumnFamilies().get(columnFamily);
    if (cf instanceof ColumnFamilyPersonality){
      return ((ColumnFamilyPersonality) cf).get(rowkey, column);
    } else {
      throw new RuntimeException("Does not support this personality");
    }
  }
  
  public void delete(String keyspace, String columnFamily, String rowkey, String column, long time){
    Keyspace ks = keyspaces.get(keyspace);
    ColumnFamily cf = ks.getColumnFamilies().get(columnFamily);
    if (cf instanceof ColumnFamilyPersonality){
      ((ColumnFamilyPersonality) cf).delete(rowkey, column, time);
    } else {
      throw new RuntimeException("Does not support this personality");
    }
  }
  
  public ConcurrentMap<String, Keyspace> getKeyspaces() {
    return keyspaces;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public CompactionManager getCompactionManager() {
    return compactionManager;
  }

  public MetaDataManager getMetaDataManager() {
    return metaDataManager;
  }

  public ServerId getServerId() {
    return serverId;
  }

  public ClusterMembership getClusterMembership() {
    return clusterMembership;
  }

  public Coordinator getCoordinator() {
    return coordinator;
  }
    
}
