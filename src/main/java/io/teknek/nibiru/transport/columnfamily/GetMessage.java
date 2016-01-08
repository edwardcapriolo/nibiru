package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.Routable;

public class GetMessage extends ColumnFamilyMessage implements Routable {
  private Consistency consistency;
  private String row;
  private String column;
  private TraceTo traceTo;
  private Long timeout;
  
  public GetMessage(){
    
  }

  public Consistency getConsistency() {
    return consistency;
  }

  public void setConsistency(Consistency consistency) {
    this.consistency = consistency;
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

  public Long getTimeout() {
    return timeout;
  }

  public void setTimeout(Long timeout) {
    this.timeout = timeout;
  }

  @Override
  public String determineRoutingInformation() {
    return row;
  }
  
}
