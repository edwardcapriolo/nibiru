package io.teknek.nibiru.engine;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.TimeSource;
import io.teknek.nibiru.TimeSourceImpl;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;

public abstract class AbstractMemtable implements Comparable<AbstractMemtable>{
  private static AtomicLong MEMTABLE_ID = new AtomicLong();
  
  protected CommitLog commitLog;
  protected Store store;
  protected final long myId;
  protected TimeSource timeSource;
  
  public AbstractMemtable(Store columnFamily, CommitLog commitLog){
    this.commitLog = commitLog;
    this.store = columnFamily;
    myId = MEMTABLE_ID.getAndIncrement();
    timeSource = new TimeSourceImpl();
  }
  
  public abstract int size();
  public abstract void put(Token rowkey, String column, String value, long stamp, long ttl) ;
  public abstract AtomValue get(Token row, String column);
  public abstract SortedMap<AtomKey,AtomValue> slice(Token rowkey, String start, String end);
  public abstract void delete(Token row, long time);
  public abstract void delete (Token rowkey, String column, long time);
  
  //TODO more generic
  public abstract ConcurrentSkipListMap<Token, ConcurrentSkipListMap<AtomKey, AtomValue>> getData();
  
  public abstract Iterator<MemtablePair<Token, Map<AtomKey,Iterator<AtomValue>>>> getDataIterator();

  @Override
  public int compareTo(AbstractMemtable o) {
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
  
  //VisibileForTesting
  public TimeSource getTimeSource() {
    return timeSource;
  }
  
  //VisibileForTesting
  public void setTimeSource(TimeSource timeSource) {
    this.timeSource = timeSource;
  }  

}