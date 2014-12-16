package io.teknek.nibiru.engine;

import java.io.IOException;

public class CompactionManager {
  private ColumnFamily columnFamily;
  
  public CompactionManager(ColumnFamily columnFamily){
    this.columnFamily = columnFamily;
  }
  
  public void compact(SSTable [] ssTables) throws IOException{
    String lowestKey = null;
    SsTableStreamReader [] r = new SsTableStreamReader[ssTables.length];
    Token [] t = new Token[ssTables.length];
    for (int i=0; i<ssTables.length; i++){
      r[i] = ssTables[i].get();
    }
    
    //for (SSTable s : ssTables){
    //  s.get()
    //}
    
  }
}
