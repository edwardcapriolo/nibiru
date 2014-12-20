package io.teknek.nibiru.engine;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class CompactionManager {
  private ColumnFamily columnFamily;
  
  
  public CompactionManager(ColumnFamily columnFamily){
    this.columnFamily = columnFamily;
  }
  
  public String getNewSsTableName(){
    return String.valueOf(System.nanoTime());
  }
  
  public void compact(SsTable [] ssTables) throws IOException {
    SsTableStreamReader[] readers = new SsTableStreamReader[ssTables.length];
    SsTableStreamWriter newSsTable = new SsTableStreamWriter(getNewSsTableName(), 
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
  }
  
  private void advance(Token lowestToken, SsTableStreamReader[] r, Token[] t) throws IOException{
    for (int i = 0; i < t.length; i++) {
      if (t[i] != null && t[i].getToken().equals(lowestToken.getToken())){
        t[i] = r[i].getNextToken();
      }
    }
  }
  
  private void merge(SortedMap<String,Val> allColumns, SortedMap<String,Val> otherColumns){
    for (Map.Entry<String,Val> column: otherColumns.entrySet()){
      Val existing = allColumns.get(column.getKey());
      if (existing == null) {
        allColumns.put(column.getKey(), column.getValue());
      } else if (existing.getTime() < column.getValue().getTime()){
        allColumns.put(column.getKey(), column.getValue());
      }  // we should handle the equal/tombstone case here
    }
    
  }
  
  private Token lowestToken(Token [] t){
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
  
  private boolean allNull(Token[] t){
    for (Token j : t){
      if (j != null){
        return false;
      }
    }
    return true;
  }
}
