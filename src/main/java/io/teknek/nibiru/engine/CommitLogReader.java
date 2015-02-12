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

import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.io.BufferGroup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedMap;

public class CommitLogReader {

  private RandomAccessFile ssRaf;
  private FileChannel ssChannel;
  private MappedByteBuffer ssBuffer;
  private BufferGroup bg;
  private final String id;
  private final DefaultColumnFamily columnFamily;
  private final File sstable;
  
  public CommitLogReader(String id, DefaultColumnFamily columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
    sstable = new File(CommitLog.getCommitLogDirectoryForColumnFamily(columnFamily), id + "." + CommitLog.EXTENSION);
  }
  
  public void open() throws IOException {
    ssRaf = new RandomAccessFile(sstable, "r");
    ssChannel = ssRaf.getChannel();
    ssBuffer = ssChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssChannel.size());
    bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    bg.setStartOffset((int) 0);
  }
  
  public Token getNextToken() throws IOException {
    if (! (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < bg.channel.size())){
      return null;
    }
    if (bg.dst[bg.currentIndex] == SsTableReader.END_ROW){
      bg.advanceIndex();
    }
    SsTableReader.readHeader(bg); 
    StringBuilder token = SsTableReader.readToken(bg);
    StringBuilder rowkey = SsTableReader.readRowkey(bg);
    Token t = new Token();
    t.setRowkey(rowkey.toString());
    t.setToken(token.toString());
    return t;
  }
  
  public SortedMap<AtomKey,Val>  readColumns() throws IOException {
    SortedMap<AtomKey,Val> columns = SsTableReader.readColumns(bg);
    return columns;
  }
  
  public void close() throws IOException {
    ssRaf.close();
    ssChannel.close();
  }
  
  public void delete(){
    sstable.delete();
  }
  
}
