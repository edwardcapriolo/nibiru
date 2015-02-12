package io.teknek.nibiru.engine.atom;

public abstract class AtomKey implements Comparable<AtomKey> {

  public abstract byte [] externalize();
}