package io.teknek.nibiru;

import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
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
  
  public void createColumnFamily(String name, Map<String,Object> properties){
    ColumnFamilyMetaData cfmd = new ColumnFamilyMetaData();
    cfmd.setName(name);
    String implementingClass = (String) properties.get(ColumnFamilyMetaData.IMPLEMENTING_CLASS);
    if (implementingClass == null){
      throw new RuntimeException("property "+ ColumnFamilyMetaData.IMPLEMENTING_CLASS + " must be specified");
    }
    cfmd.setImplementingClass(implementingClass);
    ColumnFamily columnFamily = null;
    try {
      Class<?> cfClass = Class.forName(implementingClass);
      Constructor<?> cons = cfClass.getConstructor(Keyspace.class, ColumnFamilyMetaData.class);
      columnFamily = (ColumnFamily) cons.newInstance(this, cfmd);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
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
