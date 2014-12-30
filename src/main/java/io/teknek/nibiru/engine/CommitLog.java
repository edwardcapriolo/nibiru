package io.teknek.nibiru.engine;

import io.teknek.nibiru.TimeSource;
import io.teknek.nibiru.TimeSourceImpl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CommitLog {

  private final ColumnFamily columnFamily;
  private final String tableId;
  private CountingBufferedOutputStream ssOutputStream;
  private long lastOffset = 0;
  private File sstableFile;
  private TimeSource timeSource = new TimeSourceImpl();
  public static final String EXTENSION = "commitlog";
  
  public CommitLog(ColumnFamily cf){
    this.columnFamily = cf;
    tableId = String.valueOf(timeSource.getTimeInMillis());
  }
  
  public static File getCommitLogDirectoryForColumnFamily(ColumnFamily columnFamily){
    return new File(columnFamily.getKeyspace().getConfiguration()
            .getCommitlogDirectory(), 
            columnFamily.getColumnFamilyMetadata()
            .getName());
  }
  
  public void open() throws FileNotFoundException {
    if (!getCommitLogDirectoryForColumnFamily(columnFamily).exists()){
      boolean mkdir = getCommitLogDirectoryForColumnFamily(columnFamily).mkdirs();
      if (!mkdir){
        throw new RuntimeException("Could not create directory");
      }
    }
    sstableFile = new File(getCommitLogDirectoryForColumnFamily(columnFamily),  tableId + "." + EXTENSION);
    ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
  }
  
  public synchronized void write(Token rowkey, String column, String value, long stamp, long ttl)
          throws IOException {
    Map<String, Val> columns = new HashMap<>();
    Val v = new Val(value, stamp, System.currentTimeMillis(), ttl);
    columns.put(column, v);
    ssOutputStream.writeAndCount(SsTableReader.START_RECORD);
    ssOutputStream.writeAndCount(rowkey.getToken().getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_TOKEN);
    ssOutputStream.writeAndCount(rowkey.getRowkey().getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_ROWKEY);
    boolean writeJoin = false;
    for (Entry<String, Val> j : columns.entrySet()) {
      if (!writeJoin) {
        writeJoin = true;
      } else {
        ssOutputStream.writeAndCount(SsTableReader.END_COLUMN);
      }
      ssOutputStream.writeAndCount(j.getKey().getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getCreateTime()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTime()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTtl()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getValue()).getBytes());
    }
    ssOutputStream.writeAndCount(SsTableReader.END_ROW);
    
    if (columnFamily.getColumnFamilyMetadata().getCommitlogFlushBytes() > 0 && 
            ssOutputStream.getWrittenOffset() - lastOffset > columnFamily.getColumnFamilyMetadata().getCommitlogFlushBytes()){
      ssOutputStream.flush();
      lastOffset = ssOutputStream.getWrittenOffset();
    }
  }

  public void delete() throws IOException {
    ssOutputStream.close();
    sstableFile.delete();
  }

  public void close() throws IOException {
    ssOutputStream.close();
  }
}
