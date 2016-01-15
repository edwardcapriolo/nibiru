package io.teknek.nibiru.engine;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.engine.Memtable;

public class MemtableTest extends AbstractMemtableTest {
  
  @Override
  public AbstractMemtable makeMemtable(Keyspace ks1) {
    return new Memtable( ks1.getStores().get("abc"), new CommitLog(ks1.getStores().get("abc")));
  }
  
}
