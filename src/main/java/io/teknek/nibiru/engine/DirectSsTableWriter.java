package io.teknek.nibiru.engine;

import java.util.SortedMap;

import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;

/** API is used to bulk write sstables. No data is visible until the sstable is succefully closed **/
public interface DirectSsTableWriter {

  static final String PERSONALITY = "DIRECT_SSTABLE_PERSONALITY";
  static final String OPEN = "OPEN";
  static final String WRITE = "WRITE";
  static final String CLOSE = "CLOSE";
  
  void open(String id);
  
  void write(Token token, SortedMap<AtomKey,AtomValue> columns, String id);
  
  /* close the table and make it live (readable)*/
  void close(String id);
  
}
