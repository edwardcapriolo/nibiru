package io.teknek.nibiru;

import io.teknek.nibiru.engine.DefaultColumnFamily;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class TombstoneReaper implements Runnable {

  private final DefaultColumnFamily columnFamily;
  private volatile boolean goOn;
  
  public TombstoneReaper(DefaultColumnFamily columnFamily){
    this.columnFamily = columnFamily;
    goOn = true;
  }
  
  @Override
  public void run() {
    while(goOn){
      long now = System.currentTimeMillis();
      processColumnFamily(this.columnFamily, now);
      try {
        Thread.sleep(2);
      } catch (InterruptedException e) {
      }
    }
  }
  
  //VisibleForTesting
  public void processColumnFamily(ColumnFamily columnFamily, long currentTimeMillis){
    long graceMillis = columnFamily.getColumnFamilyMetadata().getTombstoneGraceMillis();
    for (Entry<Token, ConcurrentSkipListMap<String, Val>> entry : ((DefaultColumnFamily) columnFamily).getMemtable().getData().entrySet()){
      for (Map.Entry<String, Val> innerEntry : entry.getValue().entrySet()){
        if (innerEntry.getValue().getValue() == null){
          if (innerEntry.getValue().getCreateTime() + graceMillis < currentTimeMillis){
            entry.getValue().remove(innerEntry.getKey());
          }
        }
      }
    }
  }

  public boolean isGoOn() {
    return goOn;
  }

  public void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }
  
}
