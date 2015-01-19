/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru.engine;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;

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
