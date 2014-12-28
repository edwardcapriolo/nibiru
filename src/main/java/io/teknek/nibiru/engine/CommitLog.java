package io.teknek.nibiru.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class CommitLog {

  private final ColumnFamily columnFamily;
  private final File cfCommitlogDirectory;
  private final String tableId;
  private CountingBufferedOutputStream ssOutputStream;
  private long lastOffset = 0;
  private File sstableFile;
  
  public CommitLog(ColumnFamily cf){
    this.columnFamily = cf;
    cfCommitlogDirectory = new File(columnFamily.getKeyspace().getConfiguration()
            .getCommitlogDirectory(), 
            columnFamily.getColumnFamilyMetadata()
            .getName());
    tableId = String.valueOf(System.nanoTime());
    
  }
  
  public void open() throws FileNotFoundException {
    if (!cfCommitlogDirectory.exists()){
      boolean mkdir = cfCommitlogDirectory.mkdirs();
      if (!mkdir){
        throw new RuntimeException("Could not create directory");
      }
    }
    sstableFile = new File(this.cfCommitlogDirectory, columnFamily.getColumnFamilyMetadata().getName() + "-" + tableId + ".commit");
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
    if (ssOutputStream.getWrittenOffset() - lastOffset > 1000){
      ssOutputStream.flush();
      lastOffset = ssOutputStream.getWrittenOffset();
    }
  }

  public void delete() throws IOException {
    ssOutputStream.close();
    sstableFile.delete();
  }

}

    /*  
    public void run(){
      while (true){
        boolean anyTableUnflushed = false;
        for (Memtable memtable : associatedMemtables){
          if (columnFamily.getMemtable().compareTo(memtable) == 0){
            anyTableUnflushed = true;
          }
        }
        for (Memtable memtable : associatedMemtables){
          if (this.columnFamily.getMemtableFlusher().getMemtables().contains(memtable)){
            anyTableUnflushed = true;
          }
        }
        if (!anyTableUnflushed){
          System.out.println("Deleting commit log");
          //commitlogs.delete
        } else {
          System.out.println("cant flush commit log");
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          
        }
      }
    }
    */

/*  
public void run(){
  while (true){
    boolean anyTableUnflushed = false;
    for (Memtable memtable : associatedMemtables){
      if (columnFamily.getMemtable().compareTo(memtable) == 0){
        anyTableUnflushed = true;
      }
    }
    for (Memtable memtable : associatedMemtables){
      if (this.columnFamily.getMemtableFlusher().getMemtables().contains(memtable)){
        anyTableUnflushed = true;
      }
    }
    if (!anyTableUnflushed){
      System.out.println("Deleting commit log");
      //commitlogs.delete
    } else {
      System.out.println("cant flush commit log");
    }
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      
    }
  }
}
*/
