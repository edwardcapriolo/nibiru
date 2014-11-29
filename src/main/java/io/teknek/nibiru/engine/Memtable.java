package io.teknek.nibiru.engine;


import io.teknek.nibiru.TimeSource;
import io.teknek.nibiru.TimeSourceImpl;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Memtable {

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<String,Val>> data;
  private TimeSource timeSource;
  
  public Memtable(){
    data = new ConcurrentSkipListMap<>();
    timeSource = new TimeSourceImpl();
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
  
}
