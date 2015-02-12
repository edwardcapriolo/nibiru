package io.teknek.nibiru.engine.atom;

/**
 * We want the row tombstone to sit in the front of the row
 * @author edward
 *
 */
public class RowTombstoneKey extends AtomKey {

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