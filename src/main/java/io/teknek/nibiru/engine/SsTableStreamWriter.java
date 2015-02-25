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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.io.CountingBufferedOutputStream;

public class SsTableStreamWriter {

  private final ColumnFamily columnFamily;
  private final String id;
  private final IndexWriter indexWriter;
  private CountingBufferedOutputStream ssOutputStream;
  private BloomFilterWriter bloomFilter;
  
  public SsTableStreamWriter(String id, ColumnFamily columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
    indexWriter = new IndexWriter(id, columnFamily);
    bloomFilter = new BloomFilterWriter(id, columnFamily.getKeyspace().getConfiguration());
  }
  
  public void open() throws FileNotFoundException {
    File sstableFile = new File(columnFamily.getKeyspace().getConfiguration().getDataDirectory(), id + ".ss");
    if (!columnFamily.getKeyspace().getConfiguration().getDataDirectory().exists()){
      boolean create = columnFamily.getKeyspace().getConfiguration().getDataDirectory().mkdirs();
      if (!create){
        throw new RuntimeException ("could not create "+ columnFamily.getKeyspace().getConfiguration().getDataDirectory());
      }
    }
    ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
    indexWriter.open();
  }
  
  public static void writeToken(Token token, CountingBufferedOutputStream ssOutputStream) throws IOException {
    ssOutputStream.writeAndCount((byte) (token.getToken().getBytes().length >> 8) & 0xFF);
    ssOutputStream.writeAndCount((byte) (token.getToken().getBytes().length & 0xFF));
    ssOutputStream.writeAndCount(token.getToken().getBytes());
  }
  
  public static void writeRowkey(Token token, CountingBufferedOutputStream ssOutputStream) throws IOException {
    ssOutputStream.writeAndCount((byte) (token.getRowkey().getBytes().length >> 8) & 0xFF);
    ssOutputStream.writeAndCount((byte) (token.getRowkey().getBytes().length & 0xFF));
    ssOutputStream.writeAndCount(token.getRowkey().getBytes());
  }
  
  public static void writeColumns(Map<AtomKey,AtomValue> columns, CountingBufferedOutputStream ssOutputStream) throws IOException {
    ssOutputStream.writeAndCount((byte) (columns.size() >> 8) & 0xFF);
    ssOutputStream.writeAndCount((byte) (columns.size() & 0xFF));
    for (Entry<AtomKey, AtomValue> j : columns.entrySet()) {
      {
        byte[] key = j.getKey().externalize();
        ssOutputStream.writeAndCount((byte) (key.length >> 8) & 0xFF);
        ssOutputStream.writeAndCount((byte) (key.length & 0xFF));
        ssOutputStream.writeAndCount(key);
      }
      {
        byte[] value = j.getValue().externalize();
        ssOutputStream.writeAndCount((byte) (value.length >> 8) & 0xFF);
        ssOutputStream.writeAndCount((byte) (value.length & 0xFF));
        ssOutputStream.writeAndCount(value);
      }
    }
  }
  
  public void write(Token t, Map<AtomKey,AtomValue> columns) throws IOException {
    long startOfRecord = ssOutputStream.getWrittenOffset();
    bloomFilter.put(t);
    ssOutputStream.writeAndCount(SsTableReader.START_RECORD);
    writeToken(t, ssOutputStream);
    writeRowkey(t, ssOutputStream);
    indexWriter.handleRow(startOfRecord, t.getToken());
    writeColumns(columns, ssOutputStream);
    ssOutputStream.writeAndCount(SsTableReader.END_ROW);
  }
  
  public void close() throws IOException {
    indexWriter.close();
    ssOutputStream.close();
    bloomFilter.writeAndClose();
  }
}
