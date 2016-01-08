package io.teknek.nibiru.transport.columnfamilyadmin;

public class CleanupMessage extends ColumnFamilyAdminMessage {

  private String keyspace;
  private String columnFamily;
  
  public CleanupMessage(){
    
  }

  public String getKeyspace() {
    return keyspace;
  }

  public void setKeyspace(String keyspace) {
    this.keyspace = keyspace;
  }

  public String getColumnFamily() {
    return columnFamily;
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
  }
}
