package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.Routable;

public class GetMessage extends ColumnFamilyMessage implements Routable {
  private String row;
  private String column;
  private TraceTo traceTo;
  
  public GetMessage(){
    
  }

  public String getRow() {
    return row;
  }

  public void setRow(String row) {
    this.row = row;
  }

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public TraceTo getTraceTo() {
    return traceTo;
  }

  public void setTraceTo(TraceTo traceTo) {
    this.traceTo = traceTo;
  }

  @Override
  public String determineRoutingInformation() {
    return row;
  }
  
}
