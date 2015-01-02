package io.teknek.nibiru.engine;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTableWriter {

  public void flushToDisk(String id, ColumnFamily columnFamily, Memtable m) throws IOException{
    SsTableStreamWriter w = new SsTableStreamWriter(id, columnFamily);
    w.open();
    for (Entry<Token, ConcurrentSkipListMap<String, Val>> i : m.getData().entrySet()){
      w.write(i.getKey(), i.getValue());
    }
    w.close();
  }
}