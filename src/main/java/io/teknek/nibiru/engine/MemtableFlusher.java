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

import java.io.IOException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

public class MemtableFlusher implements Runnable {
  private ConcurrentSkipListSet<Memtable> memtables = new ConcurrentSkipListSet<>();
  private DefaultColumnFamily columnFamily;
  private Thread myThread;
  private AtomicLong flushes;
  private volatile boolean goOn = true;
  
  public MemtableFlusher(DefaultColumnFamily columnFamily){
    this.columnFamily = columnFamily;
    flushes = new AtomicLong(0);
  }
  
  public boolean add(Memtable memtable){
    return memtables.add(memtable);
  }

  public ConcurrentSkipListSet<Memtable> getMemtables() {
    return memtables;
  }

  public void start(){
    myThread = new Thread(this);
    myThread.start();
  }
  
  public void doBlockingFlush(){
    for (Memtable memtable : memtables){
      SSTableWriter ssTableWriter = new SSTableWriter();
      try {
        //TODO: a timeuuid would be better here
        String tableId = String.valueOf(System.nanoTime());
        ssTableWriter.flushToDisk(tableId, columnFamily, memtable);
        SsTable table = new SsTable(columnFamily);
        table.open(tableId, columnFamily.getKeyspace().getConfiguration());
        columnFamily.getSstable().add(table);
        memtables.remove(memtable);
        memtable.getCommitLog().delete();
        flushes.incrementAndGet();
      } catch (IOException e) {
        //TODO: catch this and terminate server?
        throw new RuntimeException(e);
      }
    }
  }
  @Override
  public void run() {
    while (goOn){
      doBlockingFlush();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
  
  public long getFlushCount(){
    return flushes.get();
  }

  public boolean isGoOn() {
    return goOn;
  }

  public void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }
  
}
