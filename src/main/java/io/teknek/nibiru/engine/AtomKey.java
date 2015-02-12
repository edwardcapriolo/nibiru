package io.teknek.nibiru.engine;

public abstract class AtomKey implements Comparable<AtomKey> {

  public abstract byte [] externalize();
}

/**
 * We want the row tombstone to sit in the front of the row
 * @author edward
 *
 */
class RowTombstoneKey extends AtomKey {

  @Override
  public int compareTo(AtomKey o) {
    if (o instanceof RowTombstoneKey){
      return 0;
    }
    return -1;
  }

  @Override
  public byte[] externalize() {

    return "T".getBytes();
  }
  
}

class RangeTombstoneKey extends AtomKey {

  private final String startRange;
  private final String endRange;
  
  public RangeTombstoneKey(String startRange, String endRange){
    this.startRange = startRange;
    this.endRange = endRange;
  }
  
  @Override
  public int compareTo(AtomKey o) {
    if (o instanceof RowTombstoneKey){
      return 1;
    } else if (o instanceof RangeTombstoneKey){
      RangeTombstoneKey k = (RangeTombstoneKey) o;
      int keyCompare = startRange.compareTo(k.startRange);
      if (keyCompare < 0){
        return -1;
      } else if (keyCompare > 1){
        return 1;
      } else {
        return endRange.compareTo(k.endRange);
      }
    } else if (o instanceof ColumnKey){
      return -1;
    }
    throw new IllegalArgumentException("Uncomparable...that is what you are");
  }

  @Override
  public byte[] externalize() {
    return "R".getBytes();
  }
  
}