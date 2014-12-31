package io.teknek.nibiru;

import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.ColumnFamilyMetadata;
import io.teknek.nibiru.metadata.KeyspaceMetadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Keyspace {

  private KeyspaceMetadata keyspaceMetadata;
  private Configuration configuration;
  private ConcurrentMap<String,ColumnFamily> columnFamilies;
  
  public Keyspace(Configuration configuration){
    columnFamilies = new ConcurrentHashMap<>();
    this.configuration = configuration;
  }

  public KeyspaceMetadata getKeyspaceMetadata() {
    return keyspaceMetadata;
  }

  public void setKeyspaceMetadata(KeyspaceMetadata keyspaceMetadata) {
    this.keyspaceMetadata = keyspaceMetadata;
  }
  
  public void createColumnFamily(String name){
    ColumnFamilyMetadata cfmd = new ColumnFamilyMetadata();
    cfmd.setName(name);
    cfmd.setImplementingClass(DefaultColumnFamily.class.getName());
    ColumnFamily columnFamily = null;
    try {
      Class<?> cfClass = Class.forName(DefaultColumnFamily.class.getName());
      Constructor<?> cons = cfClass.getConstructor(Keyspace.class, ColumnFamilyMetadata.class);
      columnFamily = (ColumnFamily) cons.newInstance(this, cfmd);
    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    columnFamily = new DefaultColumnFamily(this, cfmd);
    columnFamilies.put(name, columnFamily);
    keyspaceMetadata.getColumnFamilyMetaData().put(name, cfmd);
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
