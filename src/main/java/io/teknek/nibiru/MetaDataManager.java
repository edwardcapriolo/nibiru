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
  
  private void createKeyspaces(){
    Map<String,KeyspaceAndColumnFamilyMetaData> meta = read(); 
    if (!(meta == null)){
      for (Entry<String, KeyspaceAndColumnFamilyMetaData> keyspaceEntry : meta.entrySet()){
        Keyspace k = new Keyspace(configuration);
        k.setKeyspaceMetadata(keyspaceEntry.getValue().getKeyspaceMetaData());
        for (Map.Entry<String, ColumnFamilyMetaData> columnFamilyEntry : keyspaceEntry.getValue().getColumnFamilies().entrySet()){
          ColumnFamily columnFamily = null;
          try {
            Class<?> cfClass = Class.forName(columnFamilyEntry.getValue().getImplementingClass());
            Constructor<?> cons = cfClass.getConstructor(Keyspace.class, ColumnFamilyMetaData.class);
            columnFamily = (ColumnFamily) cons.newInstance(k, columnFamilyEntry.getValue());
          } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
          }
          k.getColumnFamilies().put(columnFamilyEntry.getKey(), columnFamily);
          try {
            columnFamily.init();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        server.getKeyspaces().put(keyspaceEntry.getKey(), k);
      }
    }
  }
  
  public void createKeyspace(String keyspaceName, Map<String,Object> properties){
    KeyspaceMetaData kmd = new KeyspaceMetaData(keyspaceName);
    Keyspace keyspace = new Keyspace(configuration);
    keyspace.setKeyspaceMetadata(kmd);
    server.getKeyspaces().put(keyspaceName, keyspace);
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
  
  public void createColumnFamily(String keyspaceName, String columnFamilyName, Map<String,Object> properties){
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
