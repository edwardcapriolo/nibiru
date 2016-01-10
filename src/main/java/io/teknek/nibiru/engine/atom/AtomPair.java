package io.teknek.nibiru.engine.atom;


public class AtomPair {
  private AtomKey key;

  private AtomValue value;

  public AtomPair() {

  }

  public AtomPair(AtomKey k, AtomValue v) {
    key = k;
    value = v;
  }

  public AtomKey getKey() {
    return key;
  }

  public void setKey(AtomKey key) {
    this.key = key;
  }

  public AtomValue getValue() {
    return value;
  }

  public void setValue(AtomValue value) {
    this.value = value;
  }

}