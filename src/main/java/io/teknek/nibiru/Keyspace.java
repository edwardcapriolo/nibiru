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

import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Keyspace {

  private KeyspaceMetaData keyspaceMetadata;
  private Configuration configuration;
  private ConcurrentMap<String,Store> stores;
  
  public Keyspace(Configuration configuration){
    this.configuration = configuration;
    stores = new ConcurrentHashMap<>();
  }

  public KeyspaceMetaData getKeyspaceMetadata() {
    return keyspaceMetadata;
  }

  public void setKeyspaceMetadata(KeyspaceMetaData keyspaceMetadata) {
    this.keyspaceMetadata = keyspaceMetadata;
  }
  
  public void createStore(String name, Map<String,Object> properties){
    StoreMetaData cfmd = new StoreMetaData();
    cfmd.setName(name);
    String implementingClass = (String) properties.get(StoreMetaData.IMPLEMENTING_CLASS);
    if (implementingClass == null){
      throw new RuntimeException("property "+ StoreMetaData.IMPLEMENTING_CLASS + " must be specified");
    }
    cfmd.setImplementingClass(implementingClass);
    Store columnFamily = null;
    try {
      Class<?> cfClass = Class.forName(implementingClass);
      Constructor<?> cons = cfClass.getConstructor(Keyspace.class, StoreMetaData.class);
      columnFamily = (Store) cons.newInstance(this, cfmd);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    stores.put(name, columnFamily);
  }

  public ConcurrentMap<String, Store> getStores() {
    return stores;
  }

  public void setStores(ConcurrentMap<String, Store> stores) {
    this.stores = stores;
  }
  
  public Token createToken(String rowkey){
    return keyspaceMetadata.getPartitioner().partition(rowkey);
  }

  public Configuration getConfiguration() {
    return configuration;
  }
  
}
