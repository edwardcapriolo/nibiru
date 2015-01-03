package io.teknek.nibiru;

import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Keyspace {

  private KeyspaceMetaData keyspaceMetadata;
  private Configuration configuration;
  private ConcurrentMap<String,ColumnFamily> columnFamilies;
  
  public Keyspace(Configuration configuration){
    columnFamilies = new ConcurrentHashMap<>();
    this.configuration = configuration;
  }

  public KeyspaceMetaData getKeyspaceMetadata() {
    return keyspaceMetadata;
  }

  public void setKeyspaceMetadata(KeyspaceMetaData keyspaceMetadata) {
    this.keyspaceMetadata = keyspaceMetadata;
  }
  
  public void createColumnFamily(String name){
    ColumnFamilyMetaData cfmd = new ColumnFamilyMetaData();
    cfmd.setName(name);
    cfmd.setImplementingClass(DefaultColumnFamily.class.getName());
    ColumnFamily columnFamily = null;
    try {
      Class<?> cfClass = Class.forName(DefaultColumnFamily.class.getName());
      Constructor<?> cons = cfClass.getConstructor(Keyspace.class, ColumnFamilyMetaData.class);
      columnFamily = (ColumnFamily) cons.newInstance(this, cfmd);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    columnFamily = new DefaultColumnFamily(this, cfmd);
    columnFamilies.put(name, columnFamily);
  }

  public ConcurrentMap<String, ColumnFamily> getColumnFamilies() {
    return columnFamilies;
  }

  public void setColumnFamilies(ConcurrentMap<String, ColumnFamily> columnFamilies) {
    this.columnFamilies = columnFamilies;
  }
  
  public Token createToken(String rowkey){
    return keyspaceMetadata.getPartitioner().partition(rowkey);
  }

  public Configuration getConfiguration() {
    return configuration;
  }
  
}
