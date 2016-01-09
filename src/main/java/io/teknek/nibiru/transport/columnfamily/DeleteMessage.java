package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.Routable;
import io.teknek.nibiru.transport.keyvalue.KeyValueMessage;

public class DeleteMessage extends ColumnFamilyMessage implements Routable {

  private Consistency consistency;
  private String row;
  private String column;
  private TraceTo traceTo;
  private Long timeout;
  private Long version;
  
  public DeleteMessage(){
    
  }
  
  @Override
  public String determineRoutingInformation() {
    return row;
  }
  

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
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

 
}
