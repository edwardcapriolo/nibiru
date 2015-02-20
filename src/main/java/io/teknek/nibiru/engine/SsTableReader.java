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
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.engine.atom.RowTombstoneKey;
import io.teknek.nibiru.engine.atom.TombstoneValue;
import io.teknek.nibiru.io.BufferGroup;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedMap;
import java.util.TreeMap;

public class SsTableReader {

  public static final char START_RECORD = '\0';
  public static final char END_ROWKEY = '\2';
  public static final char END_COLUMN_PART = '\3';
  public static final char END_COLUMN = '\4';
  public static final char END_ROW = '\n';
  
  private RandomAccessFile ssRaf;
  private FileChannel ssChannel;
  private MappedByteBuffer ssBuffer;
  private RandomAccessFile indexRaf;
  private FileChannel indexChannel;
  private MappedByteBuffer indexBuffer;
  private SsTable ssTable;
  private KeyCache keyCache;
  private BloomFilter bloomFilter;
  
  public SsTableReader(SsTable ssTable, KeyCache keyCache, BloomFilter bloomFilter){
    this.ssTable = ssTable;
    this.keyCache = keyCache;
    this.bloomFilter = bloomFilter;
  }
  
  public void open(String id) throws IOException{
    File sstable = new File(ssTable.getColumnFamily().getKeyspace().getConfiguration().getDataDirectory(), id + ".ss");
    ssRaf = new RandomAccessFile(sstable, "r");
    ssChannel = ssRaf.getChannel();
    ssBuffer = ssChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssChannel.size());
    
    File index = new File(ssTable.getColumnFamily().getKeyspace().getConfiguration().getDataDirectory(), id + ".index");
    indexRaf = new RandomAccessFile(index, "r");
    indexChannel = indexRaf.getChannel();
    indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
  }
  
  public void close() throws IOException {
    ssChannel.close();
    indexChannel.close();
    ssRaf.close();
    indexRaf.close();
  }
  
  public SsTableStreamReader getStreamReader() throws IOException {
    BufferGroup bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    bg.setStartOffset((int) 0);
    return new SsTableStreamReader(bg);
  }
  
  static void readHeader(BufferGroup bg) throws IOException {
    if (bg.dst[bg.currentIndex] != '\0'){
      throw new RuntimeException("corrupt expected \\0 got " + (char) bg.dst[bg.currentIndex]  );
    }
    bg.advanceIndex();
  }
  
  static StringBuilder readToken(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    int length = (bg.dst[bg.currentIndex] & 0xFF) << 8;
    bg.advanceIndex();
    length = length + (bg.dst[bg.currentIndex] & 0xFF);
    bg.advanceIndex();
    for (int i=0;i< length;i++){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    /*
    
    while (bg.dst[bg.currentIndex] != END_TOKEN){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();*/
    return token;
  }
  
  static StringBuilder readRowkey(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    while (bg.dst[bg.currentIndex] != END_ROWKEY){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();
    return token;
  }
  
  private void skipRowkey(BufferGroup bg) throws IOException{
    do {
      bg.advanceIndex();
    } while (bg.dst[bg.currentIndex] != END_ROWKEY);
  }
  
  private static StringBuilder readColumn(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (bg.dst[bg.currentIndex] != END_COLUMN_PART){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();
    return create;
  }
 
  private static StringBuilder endColumn(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (!(bg.dst[bg.currentIndex] == END_COLUMN ||bg.dst[bg.currentIndex] == END_ROW) ){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return create;
  }

 
  
  static SortedMap<AtomKey,AtomValue> readColumns(BufferGroup bg) throws IOException {
    SortedMap<AtomKey,AtomValue> result = new TreeMap<>();
    do {
      if (bg.dst[bg.currentIndex] == END_COLUMN){
        bg.advanceIndex();
      }
      StringBuilder name = readColumn(bg);
      if (name.charAt(0)== 'C'){
        byte typeOfValue = bg.dst[bg.currentIndex]; 
        bg.advanceIndex();
        if (typeOfValue == 'C'){
          StringBuilder create = readColumn(bg);
          StringBuilder time = readColumn(bg);
          StringBuilder ttl = readColumn(bg);
          StringBuilder value = endColumn(bg);
          ColumnValue v = new ColumnValue();
          v.setValue(value.toString());
          v.setTime(Long.parseLong(time.toString()));
          v.setTtl(Long.parseLong(ttl.toString()));
          v.setCreateTime(Long.parseLong(create.toString()));
          result.put(new ColumnKey(name.substring(1)), v);
        } else if (typeOfValue == 'T'){
          StringBuilder delete = readColumn(bg);
          TombstoneValue v = new TombstoneValue(Long.parseLong(delete.toString()));
          result.put(new RowTombstoneKey(), v);
        } else {
          throw new RuntimeException("corrupt data" + ((char) typeOfValue));
        }
      } else if (name.charAt(0)=='T'){
        StringBuilder delete = readColumn(bg);
        TombstoneValue v = new TombstoneValue(Long.parseLong(delete.toString()));
        result.put(new RowTombstoneKey(), v);
      } else {
        throw new IllegalArgumentException("can not handle " + name);
      }
      
    } while (bg.dst[bg.currentIndex] != END_ROW);
    return result;
  }
  
  private void ignoreColumns(BufferGroup bg) throws IOException {
    do {
      bg.advanceIndex();
    } while (bg.dst[bg.currentIndex] != END_ROW);
  }

  public AtomValue get(Token searchToken, String column) throws IOException {
    boolean mightContain = bloomFilter.mightContain(searchToken);
    if (!mightContain) {
      return null;
    }
    BufferGroup bgIndex = new BufferGroup();
    bgIndex.channel = indexChannel;
    bgIndex.mbb = (MappedByteBuffer) indexBuffer.duplicate();
    IndexReader index = new IndexReader(bgIndex);
    
    BufferGroup bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    long startOffset = keyCache.get(searchToken.getRowkey());
    if (startOffset == -1){
      startOffset = index.findStartOffset(searchToken.getToken());
    }
    bg.setStartOffset((int)startOffset);

    do {
      if (bg.dst[bg.currentIndex] == END_ROW){
        bg.advanceIndex();
      }
      long startOfRow = bg.mbb.position() - bg.blockSize  + bg.currentIndex;
      readHeader(bg);
      StringBuilder token = readToken(bg);
      if (token.toString().equals(searchToken.getToken())){
        StringBuilder rowkey = readRowkey(bg);
        if (rowkey.toString().equals(searchToken.getRowkey())){
          keyCache.put(searchToken.getRowkey(), startOfRow);
          SortedMap<AtomKey,AtomValue> columns = readColumns(bg);
          return columns.get(new ColumnKey(column));
        } else {
          ignoreColumns(bg);
        }
      } else {
        skipRowkey(bg);
        ignoreColumns(bg);
      }
    } while (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < ssChannel.size());
    
    return null;
  }
  
  //TODO this can be more efficient
  public SortedMap<AtomKey, AtomValue> slice(Token searchToken, String start, String end) throws IOException {
    boolean mightContain = bloomFilter.mightContain(searchToken);
    if (!mightContain) {
      return null;
    }
    BufferGroup bgIndex = new BufferGroup();
    bgIndex.channel = indexChannel;
    bgIndex.mbb = (MappedByteBuffer) indexBuffer.duplicate();
    IndexReader index = new IndexReader(bgIndex);
    
    BufferGroup bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    long startOffset = keyCache.get(searchToken.getRowkey());
    if (startOffset == -1){
      startOffset = index.findStartOffset(searchToken.getToken());
    }
    bg.setStartOffset((int)startOffset);

    do {
      if (bg.dst[bg.currentIndex] == END_ROW){
        bg.advanceIndex();
      }
      long startOfRow = bg.mbb.position() - bg.blockSize  + bg.currentIndex;
      readHeader(bg);
      StringBuilder token = readToken(bg);
      if (token.toString().equals(searchToken.getToken())){
        StringBuilder rowkey = readRowkey(bg);
        if (rowkey.toString().equals(searchToken.getRowkey())){
          keyCache.put(searchToken.getRowkey(), startOfRow);
          SortedMap<AtomKey,AtomValue> columns = readColumns(bg);
          return columns.subMap(new ColumnKey(start), new ColumnKey(end));
        } else {
          ignoreColumns(bg);
        }
      } else {
        skipRowkey(bg);
        ignoreColumns(bg);
      }
    } while (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < ssChannel.size());
    
    return null;
  }
}
