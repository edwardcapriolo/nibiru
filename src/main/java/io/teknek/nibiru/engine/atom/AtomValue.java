package io.teknek.nibiru.engine.atom;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.NAME,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type")  
    @JsonSubTypes({  
        @Type(value = ColumnValue.class, name = "columnValue"),  
        @Type(value = TombstoneValue.class, name = "tombstoneValue") })  
public abstract class AtomValue {

  protected long time;
  
  public abstract byte[] externalize();
  
  public long getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = time;
  }

}
