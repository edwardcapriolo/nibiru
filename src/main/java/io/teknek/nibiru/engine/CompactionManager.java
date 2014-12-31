package io.teknek.nibiru.engine;

import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class CompactionManager implements Runnable{
  private Server server;
  private AtomicLong numberOfCompactions = new AtomicLong(0);
  private volatile boolean goOn = true;
  
  public CompactionManager(Server server){
    this.server = server;
  }
  
  @Override
  public void run() {
    while (goOn){
      for (Entry<String, Keyspace> keyspaces : server.getKeyspaces().entrySet()){
        Keyspace keyspace = keyspaces.getValue();
        for (Map.Entry<String,ColumnFamily> columnFamilies : keyspace.getColumnFamilies().entrySet()){
          Set<SsTable> tables = columnFamilies.getValue().getSstable();
          if (tables.size() >= columnFamilies.getValue().getColumnFamilyMetadata().getMaxCompactionThreshold()){
            SsTable [] ssArray = tables.toArray(new SsTable[] {});
            try {
              String newName = getNewSsTableName();
              SsTable s = compact(ssArray, newName);
              tables.add(s);
              for (SsTable table : ssArray){
                tables.remove(table);
              }
              numberOfCompactions.incrementAndGet();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      } // end for
      try {
        Thread.sleep(1L);
      } catch (InterruptedException e) {
      }
    } // end while
  }

  public  String getNewSsTableName(){
    return String.valueOf(System.nanoTime());
  }
  
  public static SsTable compact(SsTable [] ssTables, String newName) throws IOException {
    ColumnFamily columnFamily = ssTables[0].getColumnFamily();
    SsTableStreamReader[] readers = new SsTableStreamReader[ssTables.length];
    SsTableStreamWriter newSsTable = new SsTableStreamWriter(newName, 
            columnFamily.getKeyspace().getConfiguration());
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
      SortedMap<String,Val> allColumns = new TreeMap<>();
      for (int i = 0; i < currentTokens.length; i++) {
        if (currentTokens[i] != null && currentTokens[i].equals(lowestToken)) {
          SortedMap<String, Val> columns = readers[i].readColumns();
          merge(allColumns, columns);
        }
      }
      newSsTable.write(lowestToken, allColumns);
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
  
  private static void merge(SortedMap<String,Val> allColumns, SortedMap<String,Val> otherColumns){
    for (Map.Entry<String,Val> column: otherColumns.entrySet()){
      Val existing = allColumns.get(column.getKey());
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
