package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class SSTable implements Comparable<SSTable>{

  private KeyCache keyCache;
  private static AtomicLong ID = new AtomicLong();
  private long myId; 
  private ColumnFamily columnFamily;
  private SsTableReader ssTableReader;
  
  public SSTable(ColumnFamily columnFamily){
    myId = ID.getAndIncrement();
    this.columnFamily = columnFamily;
  }
  
  public void open(String id, Configuration conf) throws IOException {    
    keyCache = new KeyCache(columnFamily.getColumnFamilyMetadata().getKeyCachePerSsTable());
    ssTableReader = new SsTableReader(this, keyCache);
    ssTableReader.open(id);
  }
    
  public SsTableStreamReader get() throws IOException {
    /*
    BufferGroup bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    bg.setStartOffset((int) 0);
    return new SsTableStreamReader(bg);*/
    throw new UnsupportedOperationException();
  }
  
  public Val get (String row, String column) throws IOException{
    return ssTableReader.get(row, column);
  }

  @Override
  public int compareTo(SSTable o) {
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
