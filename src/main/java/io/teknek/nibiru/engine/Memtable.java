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

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Memtable implements Comparable<Memtable>{

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<String,Val>> data;
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
    ConcurrentSkipListMap<String,Val> newMap = new ConcurrentSkipListMap<>();
    ConcurrentSkipListMap<String,Val> foundRow = data.putIfAbsent(rowkey, newMap);
    if (foundRow == null) {
      newMap.put(column, new Val(value, stamp, timeSource.getTimeInMillis(), ttl));
    } else {
      Val v = new Val(value, stamp, timeSource.getTimeInMillis(), ttl);
      while (true){
        Val foundColumn = foundRow.putIfAbsent(column, v);
        if (foundColumn == null){
          break;
        }
        if (foundColumn.getTime() < stamp){
          boolean result = foundRow.replace(column, foundColumn, v);
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
    Map<String, Val> r = data.get(row);
    if (r == null) {
      return null;
    }
    Val tomb = r.get("");
    if (tomb == null) {
      return r.get(column);
    } else {
      Val g = r.get(column);
      if (g == null) {
        return null;
      }
      if (tomb.getTime() >= g.getTime()) {
        return null;
      } else {
        return g;
      }
    }

  }
  
  public ConcurrentNavigableMap<String,Val> slice(Token rowkey, String start, String end){
    ConcurrentSkipListMap<String, Val> row = data.get(rowkey);
    if (row == null){
      return null;
    }
    Val tomb = row.get("");
    if (tomb == null){
      ConcurrentNavigableMap<String, Val> ret = data.get(rowkey).subMap(start, end);
      return ret;
    } else {
      ConcurrentNavigableMap<String, Val> ret = data.get(rowkey).subMap(start, end);
      ConcurrentNavigableMap<String, Val> copy = new ConcurrentSkipListMap<String, Val> ();
      for (Map.Entry<String, Val> entry : ret.entrySet()){
        if (tomb.getTime() < entry.getValue().getTime()){
          copy.put(entry.getKey(), entry.getValue());
        }
      }
      return copy;
    }
  }
  
  public void delete(Token row, long time){
    put(row, "", null, time, 0L);
    ConcurrentSkipListMap<String, Val> cols = data.get(row);
    for (Map.Entry<String, Val> col : cols.entrySet()){
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

  public ConcurrentSkipListMap<Token, ConcurrentSkipListMap<String, Val>> getData() {
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
