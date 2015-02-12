package io.teknek.nibiru.engine.atom;

public class ColumnKey extends AtomKey {

  private final String column;
  
  public ColumnKey(String column){
    this.column = column;
  }
  
  public String getColumn(){
    return column;
  }
  
  @Override
  public int compareTo(AtomKey o) {
    if (o instanceof RowTombstoneKey){
      return 1;
    } else if (o instanceof RangeTombstoneKey){
      return 1;
    } else if (o instanceof ColumnKey) {
      ColumnKey c = (ColumnKey) o;
      return this.column.compareTo(c.column);
    }
    throw new IllegalArgumentException("Uncomparable...that is what you are");
  }

  @Override
  public byte[] externalize() {
    return ("C"+getColumn()).getBytes();
  }
  
}