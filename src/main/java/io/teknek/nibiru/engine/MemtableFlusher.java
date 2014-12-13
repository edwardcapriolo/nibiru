package io.teknek.nibiru.engine;

import java.util.concurrent.ConcurrentSkipListSet;

public class MemtableFlusher {
  private ConcurrentSkipListSet<Memtable> memtables = new ConcurrentSkipListSet<>();
  
  public MemtableFlusher(){
    
  }
  
  public boolean add(Memtable memtable){
    return memtables.add(memtable);
  }
}
