
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class KeyspaceMetadata {
  private String name;
  private String partitioner;
  private ConcurrentMap<String,ColumnFamilyMetadata> columnFamilyMetaData; 
  
  public KeyspaceMetadata(String name){
    this.name = name;
    this.partitioner = "md5";
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

  public String getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(String partitioner) {
    this.partitioner = partitioner;
  }
  
}
