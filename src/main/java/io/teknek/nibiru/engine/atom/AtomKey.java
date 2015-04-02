package io.teknek.nibiru.engine.atom;

import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonSubTypes.Type;
import org.codehaus.jackson.annotate.JsonTypeInfo;

@JsonTypeInfo(  
        use = JsonTypeInfo.Id.NAME,  
        include = JsonTypeInfo.As.PROPERTY,  
        property = "type")  
    @JsonSubTypes({  
        @Type(value = ColumnKey.class, name = "columnKey"),  
        @Type(value = RowTombstoneKey.class, name = "rowTombstoneKey") })  
public abstract class AtomKey implements Comparable<AtomKey> {
  public String name;

  public abstract byte [] externalize();
}