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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.metadata.KeyspaceAndStoreMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.MetaDataStorage;
import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.router.LocalRouter;

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
 
  private void addSystemKeyspace(){
    Keyspace system = new Keyspace(configuration);
    KeyspaceMetaData ksmd = new KeyspaceMetaData();
    ksmd.setName("system");
    ksmd.setPartitioner( new NaturalPartitioner());
    ksmd.setRouter(new LocalRouter());
    system.setKeyspaceMetadata(ksmd);
    {
      Map<String,Object> properties = new HashMap<>();
      properties.put("implementing_class", DefaultColumnFamily.class.getName());
      system.createStore("hints", properties);
    }
    server.getKeyspaces().put("system", system);
  }

  
  private void createKeyspaces(){
    Map<String,KeyspaceAndStoreMetaData> meta = read();
    addSystemKeyspace();
    if (!(meta == null)){
      for (Entry<String, KeyspaceAndStoreMetaData> keyspaceEntry : meta.entrySet()){
        Keyspace k = new Keyspace(configuration);
        k.setKeyspaceMetadata(keyspaceEntry.getValue().getKeyspaceMetaData());
        for (Map.Entry<String, StoreMetaData> columnFamilyEntry : keyspaceEntry.getValue().getColumnFamilies().entrySet()){
          Store columnFamily = null;
          try {
            Class<?> cfClass = Class.forName(columnFamilyEntry.getValue().getImplementingClass());
            Constructor<?> cons = cfClass.getConstructor(Keyspace.class, StoreMetaData.class);
            columnFamily = (Store) cons.newInstance(k, columnFamilyEntry.getValue());
          } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
          try {
            columnFamily.init();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          k.getStores().put(columnFamilyEntry.getKey(), columnFamily);
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
      result.getKeyspaceMetaData().setProperties(properties);
    }
    persistMetadata();
  }
  
  private void persistMetadata(){
    Map<String,KeyspaceAndStoreMetaData> meta = new HashMap<>();
    for (Map.Entry<String, Keyspace> entry : server.getKeyspaces().entrySet()){
      KeyspaceAndStoreMetaData kfmd = new KeyspaceAndStoreMetaData();
      kfmd.setKeyspaceMetaData(entry.getValue().getKeyspaceMetaData());
      for (Map.Entry<String, Store> cfEntry : entry.getValue().getStores().entrySet()){
        kfmd.getColumnFamilies().put(cfEntry.getKey(), cfEntry.getValue().getStoreMetadata());
      }
      meta.put(entry.getKey(), kfmd);
    }
    persist(meta);
  }
  
  public void createOrUpdateStore(String keyspaceName, String store, Map<String,Object> properties){
    server.getKeyspaces().get(keyspaceName).createStore(store, properties);
    persistMetadata();
  }
  
  public Collection<String> listKeyspaces(){
    return server.getKeyspaces().keySet();
  }
  
  public Collection<String> listStores(String keyspace){
    Keyspace ks = server.getKeyspaces().get(keyspace);
    return ks.getStores().keySet();
  }
  
  public KeyspaceMetaData getKeyspaceMetadata(String keyspace){
    return server.getKeyspaces().get(keyspace).getKeyspaceMetaData();
  }
  
  public Map<String,KeyspaceAndStoreMetaData> read(){
    return metaDataStorage.read(configuration);
  }
  
  public void persist(Map<String,KeyspaceAndStoreMetaData> meta){
    metaDataStorage.persist(configuration, meta);
  }
}
