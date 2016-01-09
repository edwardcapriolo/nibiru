package io.teknek.nibiru.transport.columnfamily;

import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.Routable;

public class PutMessage extends ColumnFamilyMessage implements Routable {

  private String row;
  private String column;
  private String value;
  private TraceTo traceTo;
  private Long timeout;
  private Long ttl;
  private Long version;
  
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

  public String getColumn() {
    return column;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
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

  public Long getTtl() {
    return ttl;
  }

  public void setTtl(Long ttl) {
    this.ttl = ttl;
  }

  public Long getVersion() {
    return version;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

}
