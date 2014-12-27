package io.teknek.nibiru.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CommitLog implements Runnable {

  private final ColumnFamily columnFamily;
  private final File cfCommitlogDirectory;
  private final List<Memtable> associatedMemtables;
  private Thread commitLogThread;
  
  public CommitLog(ColumnFamily cf){
    this.columnFamily = cf;
    cfCommitlogDirectory = new File(columnFamily.getKeyspace().getConfiguration()
            .getCommitlogDirectory(), 
            columnFamily.getColumnFamilyMetadata()
            .getName());
    associatedMemtables = Collections.synchronizedList(new ArrayList<Memtable>());
  }
    
  public void start(){
    commitLogThread = new Thread(this);
    commitLogThread.start();
  }
  
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
  public void write(Token rowkey, String column, String value, long stamp, long ttl) {
   
  }

  public List<Memtable> getAssociatedMemtables() {
    return associatedMemtables;
  }
  
}
