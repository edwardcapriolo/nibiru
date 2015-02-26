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
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;
import io.teknek.nibiru.engine.atom.TombstoneValue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class Memtable implements Comparable<Memtable>{

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey,AtomValue>> data;
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
    ConcurrentSkipListMap<AtomKey, AtomValue> newMap = new ConcurrentSkipListMap<>();
    newMap.put(new ColumnKey(column), new ColumnValue(value, stamp, timeSource.getTimeInMillis(),
            ttl));
    ConcurrentSkipListMap<AtomKey, AtomValue> foundRow = data.putIfAbsent(rowkey, newMap);
    if (foundRow == null) {
      return;
    }
    ColumnValue v = new ColumnValue(value, stamp, timeSource.getTimeInMillis(), ttl);
    while (true) {
      AtomValue foundColumn = foundRow.putIfAbsent(new ColumnKey(column), v);
      if (foundColumn == null) {
        return;
      }
      if (foundColumn instanceof TombstoneValue) {
        TombstoneValue tomb = (TombstoneValue) foundColumn;
        if (tomb.getTime() < v.getTime()) {
          boolean res = foundRow.replace(new ColumnKey(column), foundColumn, v);
          if (res) {
            return;
          }
        }
      }
      
      if (foundColumn instanceof ColumnValue) {
        ColumnValue orig = (ColumnValue) foundColumn;
        if (orig.getTime() < v.getTime()) {
          boolean res = foundRow.replace(new ColumnKey(column), foundColumn, v);
          if (res) {
            return;
          }
        }
      }
      

    }

  }
  
  /**
   * 
   * @param row
   * @param column
   * @return 
   *   null if row does not exist
   *   TombstoneValue if row has row tombstone >= column
   *   null if colun does not exist
   */
  public AtomValue get(Token row, String column) {
    ConcurrentSkipListMap<AtomKey, AtomValue> rowkeyAndColumns = data.get(row);
    if (rowkeyAndColumns == null) {
      return null;
    } else {
      TombstoneValue tomb = (TombstoneValue) rowkeyAndColumns.get(new RowTombstoneKey());
      if (tomb == null) {
        return rowkeyAndColumns.get(new ColumnKey(column));
      } else { 
        ColumnValue foundColumn = (ColumnValue) rowkeyAndColumns.get(new ColumnKey(column));
        if (foundColumn == null) {
          return tomb;
        } else {
          if (tomb.getTime() >= foundColumn.getTime()) {
            return tomb;
          } else {
            return foundColumn;
          }
        }
      }
    }
  }
  
  public SortedMap<AtomKey,AtomValue> slice(Token rowkey, String start, String end){
    SortedMap<AtomKey, AtomValue> row = data.get(rowkey);
    if (row == null){
      return new TreeMap<AtomKey,AtomValue>();
    }
    TombstoneValue tomb = (TombstoneValue) row.get(new RowTombstoneKey());
    if (tomb == null){
      return data.get(rowkey).subMap(new ColumnKey(start), new ColumnKey(end));
    }
    ConcurrentNavigableMap<AtomKey, AtomValue> copy = new ConcurrentSkipListMap<AtomKey,AtomValue> ();
    copy.put(new RowTombstoneKey(), tomb);
    ConcurrentNavigableMap<AtomKey, AtomValue> ret = data.get(rowkey).subMap(new ColumnKey(start), new ColumnKey(end));
    for (Entry<AtomKey, AtomValue> entry : ret.entrySet()){
      if (entry.getValue() instanceof TombstoneValue){
        TombstoneValue tv = (TombstoneValue) entry.getValue();
        if (tv.getTime() > tomb.getTime()){
          copy.put(entry.getKey(), entry.getValue());
        }
      } else if (entry.getValue() instanceof ColumnValue){
        ColumnValue v = (ColumnValue) entry.getValue();
        if (v.getTime() > tomb.getTime()){
          copy.put(entry.getKey(), entry.getValue());
        }
      } else {
        throw new RuntimeException("do not know what X is");
      }
    }
    return copy;
  }
  
  public void delete(Token row, long time){
    ConcurrentSkipListMap<AtomKey, AtomValue> delete = new ConcurrentSkipListMap<AtomKey, AtomValue>(); 
    delete.put(new RowTombstoneKey(), new TombstoneValue(time));
    ConcurrentSkipListMap<AtomKey, AtomValue> returned = data.putIfAbsent(row, delete);
    if (returned != null){
      //todo race here for newer tombstone
      returned.put(new RowTombstoneKey(), new TombstoneValue(time));
    }
    
    /*
    ConcurrentSkipListMap<AtomKey, AtomValue> cols = data.get(row);
    if (cols != null) {
      cols.put(new RowTombstoneKey(), new TombstoneValue(time));
    }
    for (Map.Entry<AtomKey, AtomValue> col : cols.entrySet()){
      //TODO
      if (col.getValue().getTime() < time){
        cols.remove(col.getKey());
      }
    }*/
  }
  
  public void delete (Token rowkey, String column, long time){
    if ("".equals(column)){
      throw new RuntimeException ("'' is not a valid column");
    }
    put(rowkey, column, null, time, 0L);
  }

  public ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey, AtomValue>> getData() {
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
