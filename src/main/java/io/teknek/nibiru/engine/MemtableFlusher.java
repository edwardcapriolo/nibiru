package io.teknek.nibiru.engine;

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;

public class MemtableFlusher implements Runnable {
  private ConcurrentSkipListSet<Memtable> memtables = new ConcurrentSkipListSet<>();
  private ColumnFamily columnFamily;
  
  public MemtableFlusher(ColumnFamily columnFamily){
    this.columnFamily = columnFamily;
  }
  
  public boolean add(Memtable memtable){
    return memtables.add(memtable);
  }

  public ConcurrentSkipListSet<Memtable> getMemtables() {
    return memtables;
  }

  @Override
  public void run() {
    while (true){
      for (Memtable m : memtables){
        SSTableWriter s = new SSTableWriter();
        try {
          //TODO: a timeuuid would be better here
          s.flushToDisk(String.valueOf(System.nanoTime()), columnFamily.getKeyspace().getConfiguration(), m);
          memtables.remove(m);
        } catch (IOException e) {
          //TODO: catch this and terminate server?
          throw new RuntimeException(e);
        }
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
  }
  
}
