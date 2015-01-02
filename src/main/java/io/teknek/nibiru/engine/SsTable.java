package io.teknek.nibiru.engine;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class SsTable implements Comparable<SsTable>{

  private KeyCache keyCache;
  private static AtomicLong ID = new AtomicLong();
  private long myId; 
  private ColumnFamily columnFamily;
  private SsTableReader ssTableReader;
  private BloomFilter bloomFilterReader;
  
  public SsTable(ColumnFamily columnFamily){
    this.columnFamily = columnFamily;
    myId = ID.getAndIncrement();
  }
  
  public void open(String id, Configuration conf) throws IOException {    
    bloomFilterReader = new BloomFilter();
    bloomFilterReader.open(id, conf);
    keyCache = new KeyCache(columnFamily.getColumnFamilyMetadata().getKeyCachePerSsTable());
    ssTableReader = new SsTableReader(this, keyCache, bloomFilterReader);
    ssTableReader.open(id);
  }
  
  public void close() throws IOException {
    ssTableReader.close();
  }
  public SsTableStreamReader getStreamReader() throws IOException {
    return ssTableReader.getStreamReader();
  }
  
  /*
   * If row and column exist return Val
   * if row is tombstoned return row tombstone
   * If column is tombstoned return column tombstone
   * In other words always return known data
   */
  public Val get(Token row, String column) throws IOException{
    return ssTableReader.get(row, column);
  }

  @Override
  public int compareTo(SsTable o) {
    if (o == this){
      return 0;
    }
    if (this.myId == o.myId){
      return 0;
    } else if (this.myId < o.myId){
      return -1;
    } else {
      return 1;
    }
  }

  public ColumnFamily getColumnFamily() {
    return columnFamily;
  }
  
}
