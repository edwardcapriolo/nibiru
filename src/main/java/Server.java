
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Server {
  
  private ConcurrentMap<String,Keyspace> keyspaces;
  
  public Server(){
    keyspaces = new ConcurrentHashMap<>();
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
  
  public void set(String keyspace, String columnFamily, String rowkey, String column, String value, long time){
    keyspaces.get(keyspace).getColumnFamilies().get(columnFamily).getMemtable()
    .put(rowkey, column, value, time, 0);
  }
  
  public Val get(String keyspace, String columnFamily, String rowkey, String column){
    return keyspaces.get(keyspace).getColumnFamilies().get(columnFamily).getMemtable().get(rowkey, column);
  }
}
