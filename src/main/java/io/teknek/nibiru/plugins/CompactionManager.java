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
package io.teknek.nibiru.plugins;

import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.coordinator.Coordinator;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.SsTable;
import io.teknek.nibiru.engine.SsTableStreamReader;
import io.teknek.nibiru.engine.SsTableStreamWriter;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionManager extends AbstractPlugin implements Runnable {
  public static final String MY_NAME = "compaction_manager";
  
  private AtomicLong numberOfCompactions = new AtomicLong(0);
  private volatile boolean goOn = true;
  private Thread thread;
  private volatile boolean cleanOutOfTokenRange = false;
  
  public CompactionManager(Server server){
    super(server);
  }
  
  @Override
  public String getName() {
    return MY_NAME; 
  }
  
  @Override
  public void init() {
    thread = new Thread(this);
    thread.start();
    
  }

  @Override
  public void shutdown() {
    this.setGoOn(false);
  }
  
  @Override
  public void run() {
    while (goOn){
      for (Entry<String, Keyspace> keyspaces : server.getKeyspaces().entrySet()){
        Keyspace keyspace = keyspaces.getValue();
        for (Map.Entry<String,Store> columnFamilies : keyspace.getStores().entrySet()){
          if (!(columnFamilies.getValue() instanceof DefaultColumnFamily)){
            continue;
          }
          DefaultColumnFamily defaultColumnFamily = ((DefaultColumnFamily) columnFamilies.getValue());
          maxCompactionThresholdCompaction(keyspace, defaultColumnFamily);
        }
      } 
      try {
        Thread.sleep(100L);
      } catch (InterruptedException e) {
      }
    } 
  }

  public void cleanupCompaction(Keyspace keyspace, DefaultColumnFamily defaultColumnFamily){
    Set<SsTable> tables = new TreeSet<>(defaultColumnFamily.getSstable());//duplicate because we will mute the collection
    for (SsTable table : tables){
      String newName = getNewSsTableName();
      try {
        SsTable [] ssArray = {table};
        SsTable s = compact(ssArray, newName, server.getServerId(), server.getCoordinator(), true, keyspace);
        defaultColumnFamily.getSstable().add(s);
        defaultColumnFamily.getSstable().remove(table);
        //todo delete old
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void majorCompaction(Keyspace keyspace, DefaultColumnFamily defaultColumnFamily){
    Set<SsTable> tables = new TreeSet<>(defaultColumnFamily.getSstable());//duplicate because we will mute the collection
    SsTable [] ssArray = tables.toArray(new SsTable[] {});
    try {
      String newName = getNewSsTableName();
      SsTable s = compact(ssArray, newName, server.getServerId(), server.getCoordinator(), cleanOutOfTokenRange, keyspace);
      defaultColumnFamily.getSstable().add(s);
      for (SsTable table : ssArray){
        defaultColumnFamily.getSstable().remove(table);
      }
      numberOfCompactions.incrementAndGet();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void maxCompactionThresholdCompaction(Keyspace keyspace, DefaultColumnFamily defaultColumnFamily){
    Set<SsTable> tables = defaultColumnFamily.getSstable();
    if (tables.size() >= defaultColumnFamily.getStoreMetadata().getMaxCompactionThreshold()){
      SsTable [] ssArray = tables.toArray(new SsTable[] {});
      try {
        String newName = getNewSsTableName();
        SsTable s = compact(ssArray, newName, server.getServerId(), server.getCoordinator(), cleanOutOfTokenRange, keyspace);
        tables.add(s);
        for (SsTable table : ssArray){
          tables.remove(table);
          //TODO table.delete ?
        }
        numberOfCompactions.incrementAndGet();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
    
  public static String getNewSsTableName(){
    return String.valueOf(System.nanoTime());
  }
  
  public static SsTable compact(SsTable [] ssTables, String newName, ServerId serverId,
          Coordinator coordinator, boolean cleanOutOfRange, Keyspace keyspace) throws IOException {
    
    DefaultColumnFamily columnFamily = (DefaultColumnFamily) ssTables[0].getColumnFamily();
    SsTableStreamReader[] readers = new SsTableStreamReader[ssTables.length];
    SsTableStreamWriter newSsTable = new SsTableStreamWriter(newName, 
            columnFamily);
    newSsTable.open();
    Token[] currentTokens = new Token[ssTables.length];
    for (int i = 0; i < ssTables.length; i++) {
      readers[i] = ssTables[i].getStreamReader();
    }
    for (int i = 0; i < currentTokens.length; i++) {
      currentTokens[i] = readers[i].getNextToken();
    }
    while (!allNull(currentTokens)){
      Token lowestToken = lowestToken(currentTokens);
      SortedMap<AtomKey,AtomValue> allColumns = new TreeMap<>();
      for (int i = 0; i < currentTokens.length; i++) {
        if (currentTokens[i] != null && currentTokens[i].equals(lowestToken)) {
          SortedMap<AtomKey, AtomValue> columns = readers[i].readColumns();
          merge(allColumns, columns);
        }
      }
      if (cleanOutOfRange){
        if (coordinator.destinationsForToken(lowestToken, keyspace).contains(coordinator.getDestinationLocal())){
          newSsTable.write(lowestToken, allColumns);
        }
      } else {
        newSsTable.write(lowestToken, allColumns);
      }
      advance(lowestToken, readers, currentTokens);
    }
    newSsTable.close();
    SsTable s = new SsTable(columnFamily);
    s.open(newName, columnFamily.getKeyspace().getConfiguration());
    return s;
  }
  
  private static void advance(Token lowestToken, SsTableStreamReader[] r, Token[] t) throws IOException{
    for (int i = 0; i < t.length; i++) {
      if (t[i] != null && t[i].getToken().equals(lowestToken.getToken())){
        t[i] = r[i].getNextToken();
      }
    }
  }
  
  private static void merge(SortedMap<AtomKey,AtomValue> allColumns, SortedMap<AtomKey,AtomValue> otherColumns){
    //TODO better compare rulese
    for (Map.Entry<AtomKey,AtomValue> column: otherColumns.entrySet()){
      AtomValue existing = allColumns.get(column.getKey());
      if (existing == null) {
        allColumns.put(column.getKey(), column.getValue());
      } else if (existing.getTime() < column.getValue().getTime()){
        allColumns.put(column.getKey(), column.getValue());
      }  // we should handle the equal/tombstone case here
    }
  }
  
  private static Token lowestToken(Token [] t){
    Token lowestToken = null;
    for (Token j: t){
      if (lowestToken == null){
        lowestToken = j;
      } else {
        if (j.compareTo(lowestToken) == -1) {
          lowestToken = j;
        }
      }
    }
    return lowestToken;
  }
  
  private static boolean allNull(Token[] t){
    for (Token j : t){
      if (j != null){
        return false;
      }
    }
    return true;
  }

  public long getNumberOfCompactions() {
    return numberOfCompactions.get();
  }

  public boolean isGoOn() {
    return goOn;
  }

  public void setGoOn(boolean goOn) {
    this.goOn = goOn;
  }
  
}
