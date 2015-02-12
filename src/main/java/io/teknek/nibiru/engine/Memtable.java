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
import io.teknek.nibiru.TimeSource;
import io.teknek.nibiru.TimeSourceImpl;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Memtable implements Comparable<Memtable>{

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey,Val>> data;
  private TimeSource timeSource;
  private final ColumnFamily columnFamily;
  private final long myId;
  private static AtomicLong MEMTABLE_ID = new AtomicLong();
  private final CommitLog commitLog;
  
  public Memtable(ColumnFamily columnFamily, CommitLog commitLog){
    data = new ConcurrentSkipListMap<>();
    timeSource = new TimeSourceImpl();
    this.columnFamily = columnFamily;
    myId = MEMTABLE_ID.getAndIncrement();
    this.commitLog = commitLog;
  }
  
  public int size(){
    return data.size();
  }
  
  public void put(Token rowkey, String column, String value, long stamp, long ttl) {
    ConcurrentSkipListMap<AtomKey,Val> newMap = new ConcurrentSkipListMap<>();
    ConcurrentSkipListMap<AtomKey,Val> foundRow = data.putIfAbsent(rowkey, newMap);
    newMap.put(new ColumnKey(column), new Val(value, stamp, timeSource.getTimeInMillis(), ttl));
    if (foundRow == null) {
      //nothing
    } else {
      Val v = new Val(value, stamp, timeSource.getTimeInMillis(), ttl);
      while (true){
        Val foundColumn = foundRow.putIfAbsent(new ColumnKey(column), v);
        if (foundColumn == null){
          break;
        }
        if (foundColumn.getTime() < stamp){
          boolean result = foundRow.replace(new ColumnKey(column), foundColumn, v);
          if (result) {
            break;
          }
        } else {
          break;
        }
      }
    }
  }
  
  public Val get(Token row, String column) {
    ConcurrentSkipListMap<AtomKey, Val> r = data.get(row);
    if (r == null) {
      return null;
    }
    Val tomb = r.get(new RowTombstoneKey());
    if (tomb == null) {
      return r.get(new ColumnKey(column));
    } 
    Val g = r.get(new ColumnKey(column));
    if (g == null) {
      return null;
    }
    if (tomb.getTime() >= g.getTime()) {
      return null;
    } else {
      return g;
    }
   
  }
  
  public SortedMap<AtomKey,Val> slice(Token rowkey, String start, String end){
    SortedMap<AtomKey, Val> row = data.get(rowkey);
    if (row == null){
      return new TreeMap<AtomKey,Val>();
    }
    ///
    Val tomb = row.get(new RowTombstoneKey());
    if (tomb == null){
      return data.get(rowkey).subMap(new ColumnKey(start), new ColumnKey(end));
    } else { 
      ConcurrentNavigableMap<AtomKey, Val> ret = data.get(rowkey).subMap(new ColumnKey(start), new ColumnKey(end));
      ConcurrentNavigableMap<AtomKey, Val> copy = new ConcurrentSkipListMap<AtomKey, Val> ();
      for (Map.Entry<AtomKey, Val> entry : ret.entrySet()){
        if (tomb.getTime() < entry.getValue().getTime()){
          copy.put(entry.getKey(), entry.getValue());
        }
      }
      return copy;
    }
  }
  
  public void delete(Token row, long time){
    //put(row, "", null, time, 0L);
    ConcurrentSkipListMap<AtomKey, Val> cols = data.get(row);
    if (cols != null) {
      cols.put(new RowTombstoneKey(), new Val(null,time, System.currentTimeMillis(), 0L));
    }
    for (Map.Entry<AtomKey, Val> col : cols.entrySet()){
      if (col.getValue().getTime() < time){
        cols.remove(col.getKey());
      }
    }
  }
  
  public void delete (Token rowkey, String column, long time){
    if ("".equals(column)){
      throw new RuntimeException ("'' is not a valid column");
    }
    put(rowkey, column, null, time, 0L);
  }

  public ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey, Val>> getData() {
    return data;
  }

  //VisibileForTesting
  public TimeSource getTimeSource() {
    return timeSource;
  }
  
  //VisibileForTesting
  public void setTimeSource(TimeSource timeSource) {
    this.timeSource = timeSource;
  }

  @Override
  public int compareTo(Memtable o) {
    if (o == this){
      return 0;
    }
    if (this.myId == o.myId){
      return 0;
    } else if (this.myId < o.myId){
      return -1;
    } else {
      return 1;
    }
  }

  public CommitLog getCommitLog() {
    return commitLog;
  }
  
}
