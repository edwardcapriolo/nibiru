package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.transport.Routable;

public class SliceMessage extends ColumnFamilyMessage implements Routable {

  private String row;
  private String start;
  private String end;
  
  public SliceMessage(){
    
  }
  
  @Override
  public String determineRoutingInformation() {
    return row;
  }

  public String getRow() {
    return row;
  }

  public void setRow(String row) {
    this.row = row;
  }

  public String getStart() {
    return start;
  }

  public void setStart(String start) {
    this.start = start;
  }

  public String getEnd() {
    return end;
  }

  public void setEnd(String end) {
    this.end = end;
  }
  

}
