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
import io.teknek.nibiru.TimeSource;
import io.teknek.nibiru.TimeSourceImpl;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.io.CountingBufferedOutputStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CommitLog {

  public static final char END_TOKEN = '\1';
  private final ColumnFamily columnFamily;
  private final String tableId;
  private CountingBufferedOutputStream ssOutputStream;
  private long lastOffset = 0;
  private File sstableFile;
  private TimeSource timeSource = new TimeSourceImpl();
  public static final String EXTENSION = "commitlog";
  
  public CommitLog(ColumnFamily cf){
    this.columnFamily = cf;
    tableId = String.valueOf(timeSource.getTimeInMillis());
  }
  
  public static File getCommitLogDirectoryForColumnFamily(ColumnFamily columnFamily){
    return new File(columnFamily.getKeyspace().getConfiguration()
            .getCommitlogDirectory(), 
            columnFamily.getColumnFamilyMetadata()
            .getName());
  }
  
  public void open() throws FileNotFoundException {
    if (!getCommitLogDirectoryForColumnFamily(columnFamily).exists()){
      boolean mkdir = getCommitLogDirectoryForColumnFamily(columnFamily).mkdirs();
      if (!mkdir){
        throw new RuntimeException("Could not create directory " + getCommitLogDirectoryForColumnFamily(columnFamily) );
      }
    }
    sstableFile = new File(getCommitLogDirectoryForColumnFamily(columnFamily),  tableId + "." + EXTENSION);
    ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
  }
  
  public synchronized void write(Token rowkey, AtomKey column, String value, long stamp, long ttl)
          throws IOException {
    Map<AtomKey, AtomValue> columns = new HashMap<>();
    ColumnValue v = new ColumnValue(value, stamp, System.currentTimeMillis(), ttl);
    columns.put(column, v);
    ssOutputStream.writeAndCount(SsTableReader.START_RECORD);
    SsTableStreamWriter.writeToken(rowkey, ssOutputStream);
    ssOutputStream.writeAndCount(rowkey.getRowkey().getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_ROWKEY);
    boolean writeJoin = false;
    for (Entry<AtomKey, AtomValue> j : columns.entrySet()) {
      if (!writeJoin) {
        writeJoin = true;
      } else {
        ssOutputStream.writeAndCount(SsTableReader.END_COLUMN);
      }
      ssOutputStream.writeAndCount(j.getKey().externalize());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      j.getValue().externalize(ssOutputStream);
    }
    ssOutputStream.writeAndCount(SsTableReader.END_ROW);
    
    if (columnFamily.getColumnFamilyMetadata().getCommitlogFlushBytes() > 0 && 
            ssOutputStream.getWrittenOffset() - lastOffset > columnFamily.getColumnFamilyMetadata().getCommitlogFlushBytes()){
      ssOutputStream.flush();
      lastOffset = ssOutputStream.getWrittenOffset();
    }
  }

  public void delete() throws IOException {
    ssOutputStream.close();
    sstableFile.delete();
  }

  public void close() throws IOException {
    ssOutputStream.close();
  }
}
