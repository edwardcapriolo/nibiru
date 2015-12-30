package io.teknek.nibiru.engine;

import io.teknek.nibiru.Keyspace;

public class VersionedMemtableTest extends AbstractMemtableTest{

  @Override
  public AbstractMemtable makeMemtable(Keyspace ks1) {
    VersionedMemtable m = new VersionedMemtable(ks1.getStores().get("abc"), 
            new CommitLog(ks1.getStores().get("abc")));
    return m;
  }
  
}
