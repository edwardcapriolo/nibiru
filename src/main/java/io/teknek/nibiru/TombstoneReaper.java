package io.teknek.nibiru;

import io.teknek.nibiru.engine.ColumnFamily;
import io.teknek.nibiru.engine.Keyspace;
import io.teknek.nibiru.engine.Token;
import io.teknek.nibiru.engine.Val;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class TombstoneReaper implements Runnable {

  private final Server server;
  private volatile boolean goOn;
  
  public TombstoneReaper(Server server){
    this.server = server;
    goOn = true;
  }
  
  @Override
  public void run() {
    while(goOn){
      for (Map.Entry<String,Keyspace> entry : server.getKeyspaces().entrySet()){
        processKeyspace(entry.getValue());
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          
        }
      }
    }
  }

  private void processKeyspace(Keyspace keyspace){
    for (Map.Entry<String, ColumnFamily> entry: keyspace.getColumnFamilies().entrySet()){
      long now = System.currentTimeMillis();
      processColumnFamily(entry.getValue(), now);
    }
  }
  
  //VisibleForTesting
  public void processColumnFamily(ColumnFamily columnFamily, long currentTimeMillis){
    long graceMillis = columnFamily.getColumnFamilyMetadata().getTombstoneGraceMillis();
    for (Entry<Token, ConcurrentSkipListMap<String, Val>> entry : columnFamily.getMemtable().getData().entrySet()){
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
