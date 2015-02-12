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
    for (Entry<Token, ConcurrentSkipListMap<AtomKey, Val>> i : m.getData().entrySet()){
      w.write(i.getKey(), i.getValue());
    }
    w.close();
  }
}