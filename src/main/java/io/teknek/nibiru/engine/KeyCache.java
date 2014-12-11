package io.teknek.nibiru.engine;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class KeyCache {
  private Cache<String,Long> lc;
  public KeyCache(int cacheSize){
    lc = CacheBuilder.newBuilder().maximumSize(cacheSize).recordStats().build();
  }
  
  public void put(String key, long value){
    lc.put(key, value);
  }
  
  public long get(String key) {
    long res = -1;
    Long l = lc.getIfPresent(key);
    if (l != null) {
      res = l.longValue();
    }
    return res;
  }
}
