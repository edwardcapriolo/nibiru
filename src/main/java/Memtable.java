

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Memtable {

  private ConcurrentSkipListMap<String, ConcurrentSkipListMap<String,Val>> data;
  
  public Memtable(){
    data = new ConcurrentSkipListMap<>();
  }
  
  public void put(String rowkey, String column, String value, long stamp, long ttl) {
    ConcurrentSkipListMap<String,Val> newMap = new ConcurrentSkipListMap<>();
    ConcurrentSkipListMap<String,Val> foundRow = data.putIfAbsent(rowkey, newMap);
    if (foundRow == null) {
      newMap.put(column, new Val(value, stamp, System.currentTimeMillis(), ttl));
    } else {
      Val v = new Val(value, stamp, System.currentTimeMillis(), ttl);
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
  
  public Val get (String row, String column){
    Map<String, Val> r = data.get(row);
    Val tomb = r.get("");
    if (r == null){
      return null;
    } else {
      if (tomb == null) { 
        return r.get(column);
      } else {
        Val g = r.get(column);
        if (tomb.getTime() >= g.getTime()){
          return null;
        } else {
          return g;
        }
      }
    }
  }
  
  public ConcurrentNavigableMap slice(String rowkey, String start, String end){
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
  
  public void delete(String row, long time){
    put(row, "", null, time, 0L);
  }
  
  public void delete (String rowkey, String column, long time){
    if ("".equals(column)){
      throw new RuntimeException ("'' is not a valid column");
    }
    put(rowkey, column, null, time, 0L);
  }
}
