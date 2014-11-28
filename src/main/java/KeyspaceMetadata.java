import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class KeyspaceMetadata {
  private String name;
  private ConcurrentMap<String,ColumnFamilyMetadata> columnFamilyMetaData; 
  
  public KeyspaceMetadata(String name){
    this.name = name;
    columnFamilyMetaData = new ConcurrentHashMap<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ConcurrentMap<String, ColumnFamilyMetadata> getColumnFamilyMetaData() {
    return columnFamilyMetaData;
  }

  public void setColumnFamilyMetaData(ConcurrentMap<String, ColumnFamilyMetadata> columnFamilyMetaData) {
    this.columnFamilyMetaData = columnFamilyMetaData;
  }


  
}
