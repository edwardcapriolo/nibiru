package io.teknek.nibiru;

import java.util.Map;

import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceAndColumnFamilyMetaData;
import io.teknek.nibiru.metadata.KeyspaceMetaData;
import io.teknek.nibiru.metadata.MetaDataStorage;

public class MetaDataManager {

  private MetaDataStorage metaDataStorage;
  private final Configuration configuration;
  
  public MetaDataManager(Configuration configuration){
    this.configuration = configuration;
  }
  
  public void init(){
    try {
      metaDataStorage = (MetaDataStorage) Class.forName(configuration.getMetaDataStorageClass()).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void createKeyspace(String keyspaceName, Map<String,Object> properties){
    
  }
  
  public void createColumnFamily(String keyspaceName, String columnFamilyName, Map<String,Object> properties){
    
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
