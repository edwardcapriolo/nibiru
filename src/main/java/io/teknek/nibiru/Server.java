package io.teknek.nibiru;

import io.teknek.nibiru.engine.CompactionManager;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceAndColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.MetaDataStorage;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Server {
  
  private final ConcurrentMap<String,Keyspace> keyspaces;
  private final Configuration configuration;
  private MetaDataManager metaDataManager;
    
  private CompactionManager compactionManager;
  private Thread compactionRunnable;
  
  public Server(Configuration configuration){
    this.configuration = configuration;
    keyspaces = new ConcurrentHashMap<String,Keyspace>();
    compactionManager = new CompactionManager(this);
    metaDataManager = new MetaDataManager(configuration, this);
  }
  
  public void init(){
    metaDataManager.init();
    //keyspaces = createKeyspaces();
    compactionRunnable = new Thread(compactionManager);
    compactionRunnable.start();
  }
 
  public void shutdown() {
    compactionManager.setGoOn(false);
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
  
  public void createKeyspace(String keyspaceName){
    KeyspaceMetaData kmd = new KeyspaceMetaData(keyspaceName);
    Keyspace keyspace = new Keyspace(configuration);
    keyspace.setKeyspaceMetadata(kmd);
    keyspaces.put(keyspaceName, keyspace);
    persistMetadata();
  }
  
  public void createColumnFamily(String keyspace, String columnFamily){
    keyspaces.get(keyspace).createColumnFamily(columnFamily);
    persistMetadata();
  }

  private void persistMetadata(){
    Map<String,KeyspaceAndColumnFamilyMetaData> meta = new HashMap<>();
    for (Map.Entry<String, Keyspace> entry : keyspaces.entrySet()){
      KeyspaceAndColumnFamilyMetaData kfmd = new KeyspaceAndColumnFamilyMetaData();
      kfmd.setKeyspaceMetaData(entry.getValue().getKeyspaceMetadata());
      for (Map.Entry<String, ColumnFamily> cfEntry : entry.getValue().getColumnFamilies().entrySet()){
        kfmd.getColumnFamilies().put(cfEntry.getKey(), cfEntry.getValue().getColumnFamilyMetadata());
      }
      meta.put(entry.getKey(), kfmd);
    }
    metaDataManager.persist(meta);
  }

  
  public void put(String keyspace, String columnFamily, String rowkey, String column, String value, long time){
    Keyspace ks = keyspaces.get(keyspace);
    ks.getColumnFamilies().get(columnFamily)
      .put(rowkey, column, value, time, 0L);
  }
  
  public void put(String keyspace, String columnFamily, String rowkey, String column, String value, long time, long ttl){
    Keyspace ks = keyspaces.get(keyspace);
    ks.getColumnFamilies().get(columnFamily).put(rowkey, column, value, time);
  }
  
  public Val get(String keyspace, String columnFamily, String rowkey, String column){
    Keyspace ks = keyspaces.get(keyspace);
    if (ks == null){
      throw new RuntimeException(keyspace + " is not found");
    }
    return ks.getColumnFamilies().get(columnFamily)
            .get(rowkey, column);
  }
  
  public void delete(String keyspace, String columnFamily, String rowkey, String column, long time){
    Keyspace ks = keyspaces.get(keyspace);
    ks.getColumnFamilies().get(columnFamily).delete(rowkey, column, time);
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
  
}
/*
 *   public ConcurrentNavigableMap<String, Val> slice(String keyspace, String columnFamily, String rowkey, String startColumn, String endColumn){
    Keyspace ks = keyspaces.get(keyspace);
    return ks.getColumnFamilies().get(columnFamily).slice(rowkey, startColumn, endColumn);
  } */

/*
public void fake_put(String keyspace, String columnFamily, String rowkey, String column, String value, long time){

   * Keyspace ks = keyspaces.get(keyspace);
   * if (ks.getCoordinator().localOnly(rowKey)){
   *   ks.getColumnFamilies().get(columnFamily)
   *    .put(rowkey, column, value, time, 0L);
   * } else {
   *   ks.getCoordinator().blocledProxiedAction(keyspace, columnFamily, rowkey, column, value, time);
   * }
  
}
    */