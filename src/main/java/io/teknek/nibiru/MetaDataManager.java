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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceAndColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.MetaDataStorage;
import io.teknek.nibiru.partitioner.Partitioner;
import io.teknek.nibiru.router.Router;

public class MetaDataManager {

  private MetaDataStorage metaDataStorage;
  private final Server server;
  private final Configuration configuration;
  
  public MetaDataManager(Configuration configuration, Server server){
    this.configuration = configuration;
    this.server = server;
  }
  
  public void init(){
    try {
      metaDataStorage = (MetaDataStorage) Class.forName(configuration.getMetaDataStorageClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    createKeyspaces();
  }
  
  private void populatePartitioner(KeyspaceMetaData keyspaceMetaData){
    try {
      Class<?> cfClass = Class.forName(keyspaceMetaData.getPartitionerClass());
      Constructor<?> cons = cfClass.getConstructor();
      Partitioner partitioner = (Partitioner) cons.newInstance();
      keyspaceMetaData.setPartitioner(partitioner);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void populateRouter(KeyspaceMetaData keyspaceMetaData){
    try {
      Class<?> cfClass = Class.forName(keyspaceMetaData.getRouterClass());
      Constructor<?> cons = cfClass.getConstructor();
      Router partitioner = (Router) cons.newInstance();
      keyspaceMetaData.setRouter(partitioner);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void createKeyspaces(){
    Map<String,KeyspaceAndColumnFamilyMetaData> meta = read(); 
    if (!(meta == null)){
      for (Entry<String, KeyspaceAndColumnFamilyMetaData> keyspaceEntry : meta.entrySet()){
        Keyspace k = new Keyspace(configuration);
        k.setKeyspaceMetadata(keyspaceEntry.getValue().getKeyspaceMetaData());
        populatePartitioner(k.getKeyspaceMetadata());
        populateRouter(k.getKeyspaceMetadata());
        for (Map.Entry<String, ColumnFamilyMetaData> columnFamilyEntry : keyspaceEntry.getValue().getColumnFamilies().entrySet()){
          ColumnFamily columnFamily = null;
          try {
            Class<?> cfClass = Class.forName(columnFamilyEntry.getValue().getImplementingClass());
            Constructor<?> cons = cfClass.getConstructor(Keyspace.class, ColumnFamilyMetaData.class);
            columnFamily = (ColumnFamily) cons.newInstance(k, columnFamilyEntry.getValue());
          } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
          try {
            columnFamily.init();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          k.getColumnFamilies().put(columnFamilyEntry.getKey(), columnFamily);
        }
        server.getKeyspaces().put(keyspaceEntry.getKey(), k);
      }
    }
  }
  
  public void createOrUpdateKeyspace(String keyspaceName, Map<String,Object> properties){
    KeyspaceMetaData kmd = new KeyspaceMetaData(keyspaceName, properties);
    Keyspace keyspace = new Keyspace(configuration);
    keyspace.setKeyspaceMetadata(kmd);
    kmd.setProperties(properties);
    Keyspace result = server.getKeyspaces().putIfAbsent(keyspaceName, keyspace);
    if (result != null){
      //TODO what if this changes partitioner etc?
      result.getKeyspaceMetadata().setProperties(properties);
    }
    persistMetadata();
  }
  
  private void persistMetadata(){
    Map<String,KeyspaceAndColumnFamilyMetaData> meta = new HashMap<>();
    for (Map.Entry<String, Keyspace> entry : server.getKeyspaces().entrySet()){
      KeyspaceAndColumnFamilyMetaData kfmd = new KeyspaceAndColumnFamilyMetaData();
      kfmd.setKeyspaceMetaData(entry.getValue().getKeyspaceMetadata());
      for (Map.Entry<String, ColumnFamily> cfEntry : entry.getValue().getColumnFamilies().entrySet()){
        kfmd.getColumnFamilies().put(cfEntry.getKey(), cfEntry.getValue().getColumnFamilyMetadata());
      }
      meta.put(entry.getKey(), kfmd);
    }
    persist(meta);
  }
  
  public void createOrUpdateColumnFamily(String keyspaceName, String columnFamilyName, Map<String,Object> properties){
    server.getKeyspaces().get(keyspaceName).createColumnFamily(columnFamilyName, properties);
    persistMetadata();
  }
  
  public ColumnFamilyMetaData getColumnFamily(String keyspaceName, String columnFamilyName){
    return null;
  }
  
  
  public Map<String,KeyspaceAndColumnFamilyMetaData> read(){
    return metaDataStorage.read(configuration);
  }
  
  public void persist(Map<String,KeyspaceAndColumnFamilyMetaData> meta){
    metaDataStorage.persist(configuration, meta);
  }
}
