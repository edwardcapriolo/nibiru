package io.teknek.nibiru.partitioner;

import io.teknek.nibiru.engine.Token;

public interface Partitioner {
  public Token partition(String in);
}
