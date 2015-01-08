package io.teknek.nibiru.keyvalue;

import java.io.IOException;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;
import io.teknek.nibiru.personality.KeyValuePersonality;

public class InMemoryKeyValue extends ColumnFamily implements KeyValuePersonality {

  private Cache<String,String> lc;
  private final int cacheSize = 100;
  
  public InMemoryKeyValue(Keyspace keyspace, ColumnFamilyMetaData cfmd) {
    super(keyspace, cfmd);
    lc = CacheBuilder.newBuilder().maximumSize(cacheSize).recordStats().build();
  }

  @Override
  public void init() throws IOException {
    
  }

  @Override
  public void shutdown() throws IOException {
    
  }

  @Override
  public void put(String key, String value) {
    lc.put(key, value);
  }

  @Override
  public String get(String key) {
    return lc.getIfPresent(key);
  }

}
