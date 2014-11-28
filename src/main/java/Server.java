
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Server {
  
  private ConcurrentMap<String,KeyspaceMetadata> keyspaces;
  private ConcurrentMap<String,ConcurrentMap<String,Memtable>> memtables;
  
  public Server(){
    keyspaces = new ConcurrentHashMap<>();
    memtables = new ConcurrentHashMap<>();
  }
  
  public void createKeyspace(String keyspace){
    KeyspaceMetadata kmd = new KeyspaceMetadata(keyspace);
    keyspaces.put(keyspace, kmd);
    memtables.put(keyspace, new ConcurrentHashMap<String,Memtable>());
  }
  
  public void createColumnFamily(String keyspace, String columnFamily){
    ColumnFamilyMetadata m = new ColumnFamilyMetadata();
    m.setName(columnFamily);
    keyspaces.get(keyspace).getColumnFamilyMetaData().put(columnFamily, m);
    memtables.get(keyspace).put(columnFamily, new Memtable());
  }
  
  public void set(String keyspace, String columnFamily, String rowkey, String column, String value, long time){
    memtables.get(keyspace).get(columnFamily).put(rowkey, column, value, time, 0);
  }
  
  public Val get(String keyspace, String columnFamily, String rowkey, String column){
    return memtables.get(keyspace).get(columnFamily).get(rowkey, column);
  }
}
