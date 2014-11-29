package io.teknek.nibiru.partitioner;

import io.teknek.nibiru.engine.Token;

public class NaturalPartitioner implements Partitioner {
  public Token partition(String in){
    Token t = new Token();
    t.setRowkey(in);
    t.setToken(in);
    return t; 
  }
}
