package io.teknek.nibiru;

import io.teknek.nibiru.engine.CompactionManager;
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
    
  private CompactionManager compactionManager;
  private Thread compactionRunnable;
  
  public Server(Configuration configuration){
    this.configuration = configuration;
    keyspaces = new ConcurrentHashMap<String,Keyspace>();
    compactionManager = new CompactionManager(this);
    metaDataManager = new MetaDataManager(configuration, this);
    coordinator = new Coordinator(this);
    transport = new HttpJsonTransport(configuration, coordinator);
    serverId = new ServerId(configuration);
  }
  
  public void init(){
    serverId.init();
    metaDataManager.init();
    transport.init();
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
    transport.shutdown();
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
  
  public Val get(String keyspace, String columnFamily, String rowkey, String column){
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