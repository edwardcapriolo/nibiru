package io.teknek.nibiru.engine;

import java.io.IOException;

public class SsTableStreamReader {

  private BufferGroup bg;
  
  public SsTableStreamReader(BufferGroup bufferGroup){
    this.bg = bufferGroup;
  }
  
  public Token getNextToken() throws IOException{
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
}
