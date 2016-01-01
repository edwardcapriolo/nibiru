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


import io.teknek.nibiru.Store;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;
import io.teknek.nibiru.engine.atom.TombstoneValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Memtable extends AbstractMemtable {

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey,AtomValue>> data;
  
  public Memtable(Store columnFamily, CommitLog commitLog){
    super(columnFamily, commitLog);
    data = new ConcurrentSkipListMap<>();
  }
  
  public int size(){
    return data.size();
  }

  public void put(Token rowkey, String column, String value, long stamp, long ttl) {
    ColumnValue v = new ColumnValue(value, stamp, timeSource.getTimeInMillis(), ttl);
    ColumnKey c = new ColumnKey(column);
    ConcurrentSkipListMap<AtomKey, AtomValue> newMap = new ConcurrentSkipListMap<>();
    newMap.put(c, v);
    ConcurrentSkipListMap<AtomKey, AtomValue> foundRow = data.putIfAbsent(rowkey, newMap);
    if (foundRow == null) {
      return;
    }

    while (true) {
      AtomValue foundColumn = foundRow.putIfAbsent(c, v);
      if (foundColumn == null) {
        return;
      }
      if (foundColumn instanceof TombstoneValue) {
        TombstoneValue tomb = (TombstoneValue) foundColumn;
        if (tomb.getTime() < v.getTime()) {
          boolean res = foundRow.replace(c, foundColumn, v);
          if (res) {
            return;
          }
        } else {
          return ;
        }
      }
      
      if (foundColumn instanceof ColumnValue) {
        ColumnValue orig = (ColumnValue) foundColumn;
        if (orig.getTime() < v.getTime()) {
          boolean res = foundRow.replace(c, foundColumn, v);
          if (res) {
            return;
          }
        }
        if (orig.getTime() == v.getTime()){
          int compare = v.getValue().compareTo(orig.getValue());
          if (compare == 0){
            return;
          } if (compare > 0){
            boolean res = foundRow.replace(c, foundColumn, v);
            if (res) {
              return;
            }
          } else {
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
   *   TombstoneValue if row column is a tombstone column
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
        AtomValue foundColumn = rowkeyAndColumns.get(new ColumnKey(column));
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
      returned.put(new RowTombstoneKey(), new TombstoneValue(time));
    }
  }
  
  public void delete (Token row, String column, long time){
    TombstoneValue v = new TombstoneValue(time);
    ColumnKey k = new ColumnKey(column);
    ConcurrentSkipListMap<AtomKey, AtomValue> delete = new ConcurrentSkipListMap<AtomKey, AtomValue>(); 
    delete.put(k, v);
    ConcurrentSkipListMap<AtomKey, AtomValue> returned = data.putIfAbsent(row, delete);
    if (returned !=null){
      returned.put(k, v);
    }
  }

  @Override
  public Iterator<MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>>> getDataIterator() {
    final Iterator<Entry<Token, ConcurrentSkipListMap<AtomKey, AtomValue>>> dataIterator = data.entrySet().iterator();
    return new Iterator<MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>>>() {

      @Override
      public boolean hasNext() {
        return dataIterator.hasNext();
      }

      @Override
      public MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>> next() {
        Entry<Token, ConcurrentSkipListMap<AtomKey, AtomValue>> row = dataIterator.next();
        Map<AtomKey, Iterator<AtomValue>> reform = new TreeMap<>();
        for (Entry<AtomKey, AtomValue> i : row.getValue().entrySet()){
          reform.put(i.getKey(), Arrays.asList(i.getValue()).iterator());
        }
        return new MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>>(row.getKey(), reform);
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub 
      }
    };
  }
  
}
