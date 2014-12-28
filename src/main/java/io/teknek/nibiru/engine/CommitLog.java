package io.teknek.nibiru.engine;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class CommitLog {

  private final ColumnFamily columnFamily;
  private final File cfCommitlogDirectory;
  
  public CommitLog(ColumnFamily cf){
    this.columnFamily = cf;
    cfCommitlogDirectory = new File(columnFamily.getKeyspace().getConfiguration()
            .getCommitlogDirectory(), 
            columnFamily.getColumnFamilyMetadata()
            .getName());

  }
    


  public void write(Token rowkey, String column, String value, long stamp, long ttl) {
   
  }

  public void delete(){
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
