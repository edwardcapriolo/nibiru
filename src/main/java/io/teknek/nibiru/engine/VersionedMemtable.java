package io.teknek.nibiru.engine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;
import io.teknek.nibiru.engine.atom.TombstoneValue;

public class VersionedMemtable extends AbstractMemtable {

  private ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey,ConcurrentLinkedQueue<AtomValue>>> data;
  
  public VersionedMemtable(Store columnFamily, CommitLog commitLog) {
    super(columnFamily, commitLog);
    data = new ConcurrentSkipListMap<>();
  }

  @Override
  public int size() {
    return data.size();
  }

  @Override
  public void put(Token row, String column, String value, long stamp, long ttl) {
    ColumnValue v = new ColumnValue(value, stamp, timeSource.getTimeInMillis(), ttl);
    ColumnKey k = new ColumnKey(column);
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> columns 
    = new ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>>();
    ConcurrentLinkedQueue<AtomValue> i = new ConcurrentLinkedQueue<AtomValue>();
    i.add(v);
    columns.put(k, i);
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> rowAlreadyExisted = data.putIfAbsent(row, columns);
    if (rowAlreadyExisted != null){
      ConcurrentLinkedQueue<AtomValue> columnAlreadyExisted = rowAlreadyExisted.putIfAbsent(k, i);
      if (columnAlreadyExisted != null){
        columnAlreadyExisted.add(v);
        optimize(columnAlreadyExisted);
      }
    } 
  }
  
  private void optimize(ConcurrentLinkedQueue<AtomValue> columnAlreadyExisted){
    if (columnAlreadyExisted.size()==1)
      return ;
    long oldest = Long.MIN_VALUE;
    for (AtomValue val : columnAlreadyExisted){
      if (val.getTime() > oldest){
        oldest = val.getTime();
      }
    }
    Iterator<AtomValue> it = columnAlreadyExisted.iterator();
    while (it.hasNext()){
      AtomValue v = it.next();
      if (v.getTime() < oldest){
        it.remove();
      }
      //TODO we can remove values shadowed by tombstones
    }
  }

  @Override
  public AtomValue get(Token row, String column) {
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> foundRow = data.get(row);
    if (foundRow == null){
      return null;
    }
    ConcurrentLinkedQueue<AtomValue> findTomb = foundRow.get(new RowTombstoneKey());
    if (findTomb == null){
      ConcurrentLinkedQueue<AtomValue> i = foundRow.get(new ColumnKey(column));
      return highest(i);
    } else {
      TombstoneValue v = highestTombstone(findTomb);
      ConcurrentLinkedQueue<AtomValue> i = foundRow.get(new ColumnKey(column));
      if (i == null){
        return v;
      }
      AtomValue k = highest(i);
      if (v.getTime() >= k.getTime()){
        return v;
      } else {
        return k;
      }
    }
  }
  
  private TombstoneValue highestTombstone(ConcurrentLinkedQueue<AtomValue> tombstoneRow){
    long highest = Long.MIN_VALUE;
    TombstoneValue t = null;
    for (AtomValue v : tombstoneRow){
      if (v.getTime() > highest){
        highest = v.getTime();
        t = (TombstoneValue) v;
      }
    }
    return t;
  }

  public static AtomValue highest(Iterator<AtomValue> columns){
    Queue<AtomValue> q = new ArrayDeque<AtomValue>();
    while (columns.hasNext()){
      q.add(columns.next());
    }
    return highest(q);
  }
  
  public static AtomValue highest(Queue<AtomValue> columns){
    AtomValue head = columns.peek();
    if (columns.size() == 1){ //optimization
      return head;
    }
    long oldest = Long.MIN_VALUE;
    List<AtomValue> oldestList = new ArrayList<>(2);
    for (AtomValue val : columns){
      if (val.getTime() > oldest){
        oldest = val.getTime();
        oldestList.clear();
        oldestList.add(val);
      } else if (val.getTime() == oldest){
        oldestList.add(val);
      }
    }
    if (oldestList.size() == 1){ //optimization
      return oldestList.get(0);
    }
    ColumnValue highest = null;
    for (AtomValue v : oldestList){
      if (v instanceof TombstoneValue){
        return v;
      } else if (v instanceof ColumnValue){
        ColumnValue v1 = (ColumnValue) v;
        if (highest == null){
          highest = v1;
        } else if (highest.getValue().compareTo(v1.getValue())>0){
          highest = v1;
        }
      }
    }
    return highest;
  }
  
  @Override
  public SortedMap<AtomKey, AtomValue> slice(Token rowkey, String start, String end) {
    SortedMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> row = data.get(rowkey);
    if (row == null){
      return new TreeMap<AtomKey,AtomValue>();
    }
    ConcurrentLinkedQueue<AtomValue> tombstoneList = row.get(new RowTombstoneKey());
    TombstoneValue tomb = null;
    if (tombstoneList != null){
      tomb = highestTombstone(tombstoneList);
    }
    ConcurrentNavigableMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> i = 
            data.get(rowkey).subMap(new ColumnKey(start), new ColumnKey(end));
    SortedMap<AtomKey,AtomValue> results = new TreeMap<>();
    for (Entry<AtomKey, ConcurrentLinkedQueue<AtomValue>> j : i.entrySet()) {
      AtomValue v = highest(j.getValue());
      if (! (v instanceof TombstoneValue)){
        if (tomb == null){
          results.put(j.getKey(), v);
        } else {
          if (tomb.getTime()< v.getTime()){
            results.put(j.getKey(), v);  
          }
        }
      }
    }
    return results;
  }

  @Override
  public void delete(Token row, long time) {
    TombstoneValue v = new TombstoneValue(time);
    RowTombstoneKey k = new RowTombstoneKey();
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> columns 
    = new ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>>();
    ConcurrentLinkedQueue<AtomValue> i = new ConcurrentLinkedQueue<AtomValue>();
    i.add(v);
    columns.put(k, i);
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> rowAlreadyExisted = data.putIfAbsent(row, columns);
    if (rowAlreadyExisted != null){
      ConcurrentLinkedQueue<AtomValue> columnAlreadyExisted = rowAlreadyExisted.putIfAbsent(k, i);
      if (columnAlreadyExisted != null){
        columnAlreadyExisted.add(v);
      }
    } 
  }

  @Override
  public void delete(Token row, String column, long time) {
    TombstoneValue v = new TombstoneValue(time);
    ColumnKey k = new ColumnKey(column);
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> columns 
    = new ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>>();
    ConcurrentLinkedQueue<AtomValue> i = new ConcurrentLinkedQueue<AtomValue>();
    i.add(v);
    columns.put(k, i);
    ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> rowAlreadyExisted = data.putIfAbsent(row, columns);
    if (rowAlreadyExisted != null){
      ConcurrentLinkedQueue<AtomValue> columnAlreadyExisted = rowAlreadyExisted.putIfAbsent(k, i);
      if (columnAlreadyExisted != null){
        columnAlreadyExisted.add(v);
      }
    } 
  }

  @Override
  public Iterator<MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>>> getDataIterator() {
    final Iterator<Entry<Token, ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>>>> i = data.entrySet().iterator();
    return new Iterator<MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>>>() {

      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>> next() {
        Entry<Token, ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>>> b = i.next();
        ConcurrentSkipListMap<AtomKey, ConcurrentLinkedQueue<AtomValue>> l = b.getValue();
        SortedMap<AtomKey, Iterator<AtomValue>> m = new TreeMap<AtomKey, Iterator<AtomValue>>();
        for (Entry<AtomKey, ConcurrentLinkedQueue<AtomValue>> x : l.entrySet()){
          m.put(x.getKey(), x.getValue().iterator());
        }
        return new MemtablePair<Token, Map<AtomKey, Iterator<AtomValue>>> (b.getKey(), m);
      }

      @Override
      public void remove() {
        
      }
    };
  }
  
}
