package io.teknek.nibiru.transport.directsstable;

import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomPair;

import java.util.List;

public class Write extends DirectSsTableMessage {

  private List<AtomPair> columns;
  private Token token;
  
  public Write(){}
  public List<AtomPair> getColumns() {
    return columns;
  }
  public void setColumns(List<AtomPair> columns) {
    this.columns = columns;
  }
  public Token getToken() {
    return token;
  }
  public void setToken(Token token) {
    this.token = token;
  }
  
}
