package io.teknek.nibiru.engine;

import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.io.BufferGroup;

import java.io.IOException;
import java.util.SortedMap;

public class SsTableStreamReader {

  private BufferGroup bg;
  
  public SsTableStreamReader(BufferGroup bufferGroup){
    this.bg = bufferGroup;
  }
  
  public Token getNextToken() throws IOException {
    if (! (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < bg.channel.size())){
      return null;
    }
    if (bg.dst[bg.currentIndex] == SsTableReader.END_ROW){
      bg.advanceIndex();
    }
    SsTableReader.readHeader(bg); 
    StringBuilder token = SsTableReader.readToken(bg);
    StringBuilder rowkey = SsTableReader.readRowkey(bg);
    Token t = new Token();
    t.setRowkey(rowkey.toString());
    t.setToken(token.toString());
    return t;
  }
  
  public SortedMap<String,Val>  readColumns() throws IOException {
    SortedMap<String,Val> columns = SsTableReader.readColumns(bg);
    return columns;
  }
  
}
