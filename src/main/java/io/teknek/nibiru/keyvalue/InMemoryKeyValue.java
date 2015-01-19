/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
