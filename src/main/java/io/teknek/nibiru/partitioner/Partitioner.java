package io.teknek.nibiru.partitioner;

import io.teknek.nibiru.Token;

public interface Partitioner {
  public Token partition(String in);
}
