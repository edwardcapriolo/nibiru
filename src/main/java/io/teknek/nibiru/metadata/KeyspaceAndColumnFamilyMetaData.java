package io.teknek.nibiru.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is to persiste and because for the server structure we do not want
 * KeyspaceMetaData to has-a columnFamilyMetaData. 
 * @author edward
 *
 */
public class KeyspaceAndColumnFamilyMetaData {
  private KeyspaceMetaData keyspaceMetaData;
  private Map<String,ColumnFamilyMetaData> columnFamilies;
  
  public KeyspaceAndColumnFamilyMetaData(){
    columnFamilies = new HashMap<>();
  }

  public KeyspaceMetaData getKeyspaceMetaData() {
    return keyspaceMetaData;
  }

  public void setKeyspaceMetaData(KeyspaceMetaData keyspaceMetaData) {
    this.keyspaceMetaData = keyspaceMetaData;
  }

  public Map<String, ColumnFamilyMetaData> getColumnFamilies() {
    return columnFamilies;
  }

  public void setColumnFamilies(Map<String, ColumnFamilyMetaData> columnFamilies) {
    this.columnFamilies = columnFamilies;
  }
  
}
