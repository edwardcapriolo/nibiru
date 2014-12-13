package io.teknek.nibiru;

import io.teknek.nibiru.engine.Keyspace;
import io.teknek.nibiru.engine.Val;
import io.teknek.nibiru.metadata.KeyspaceMetadata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

public class Server {
  
  private ConcurrentMap<String,Keyspace> keyspaces;
  private Configuration configuration;
  private TombstoneReaper tombstoneReaper;
  private Thread tombstoneRunnable;
  
  public Server(){
    configuration = new Configuration();
    keyspaces = new ConcurrentHashMap<>();
    tombstoneReaper = new TombstoneReaper(this);
  }
  
  public void init(){
    tombstoneRunnable = new Thread(tombstoneReaper);
    tombstoneRunnable.start();
  }
  
  public void createKeyspace(String keyspaceName){
    KeyspaceMetadata kmd = new KeyspaceMetadata(keyspaceName);
    Keyspace keyspace = new Keyspace();
    keyspace.setKeyspaceMetadata(kmd);
    keyspaces.put(keyspaceName, keyspace);
  }
  
  public void createColumnFamily(String keyspace, String columnFamily){
    keyspaces.get(keyspace).createColumnFamily(columnFamily);
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
    return ks.getColumnFamilies().get(columnFamily)
            .get(rowkey, column);
  }
  
  public void delete(String keyspace, String columnFamily, String rowkey, String column, long time){
    Keyspace ks = keyspaces.get(keyspace);
    ks.getColumnFamilies().get(columnFamily).delete(rowkey, column, time);
  }

  public ConcurrentNavigableMap<String, Val> slice(String keyspace, String columnFamily, String rowkey, String startColumn, String endColumn){
    Keyspace ks = keyspaces.get(keyspace);
    return ks.getColumnFamilies().get(columnFamily).slice(rowkey, startColumn, endColumn);
  }
  
  public ConcurrentMap<String, Keyspace> getKeyspaces() {
    return keyspaces;
  }

  public TombstoneReaper getTombstoneReaper() {
    return tombstoneReaper;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }
  
}
