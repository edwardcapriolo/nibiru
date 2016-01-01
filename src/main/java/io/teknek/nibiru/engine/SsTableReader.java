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
    File pathToDataDirectory = SsTableStreamWriter.pathToSsTableDataDirectory(
            ssTable.getColumnFamily().getKeyspace().getConfiguration(),
            ssTable.getColumnFamily().getStoreMetadata());
    File sstable = new File(pathToDataDirectory, id + ".ss" );
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
    int length = readTwoByteSize(bg);
    for (int i=0;i< length;i++){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return token;
  }
  
  static StringBuilder readRowkey(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    int length = readTwoByteSize(bg);
    for (int i=0;i< length;i++){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return token;
  }
  
  private void skipRowkey(BufferGroup bg) throws IOException{
    int length = readTwoByteSize(bg);
    for (int i=0;i< length;i++){
      bg.advanceIndex();
    }
  }
  
  private static StringBuilder readColumn(BufferGroup bg) throws IOException{
    StringBuilder token = new StringBuilder();
    int length = readTwoByteSize(bg);
    bg.advanceIndex();
    for (int i=0;i< length;i++){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return token;
  }
 
  private static StringBuilder endColumn(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (!(bg.dst[bg.currentIndex] == END_COLUMN ||bg.dst[bg.currentIndex] == END_ROW) ){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return create;
  }

  private static StringBuilder endColumnPart(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (!(bg.dst[bg.currentIndex] == END_COLUMN_PART) ){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return create;
  }
  
 
  public static int readTwoByteSize(BufferGroup bg) throws IOException{
    int colSize = (bg.dst[bg.currentIndex] & 0xFF) << 8;
    bg.advanceIndex();
    colSize = colSize + (bg.dst[bg.currentIndex] & 0xFF);
    bg.advanceIndex();
    return colSize;
  }
  
  static StringBuilder readNextNIntoBuilder(BufferGroup bg, int size) throws IOException{
    StringBuilder sb = new StringBuilder();
    for (int i =0;i< size ; i++){
      sb.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return sb;
  }
  
  static SortedMap<AtomKey,AtomValue> readColumns(BufferGroup bg) throws IOException {
    SortedMap<AtomKey,AtomValue> result = new TreeMap<>();
    int numberOfColumns = readTwoByteSize(bg);
    for (int i =0;i< numberOfColumns ; i++){
      int next = readTwoByteSize(bg);
      char typeOfColumn = (char) bg.dst[bg.currentIndex];
      bg.advanceIndex();
      StringBuilder name = readNextNIntoBuilder(bg, next-1);
      AtomKey columnType = null;
      if (typeOfColumn == ColumnKey.SERIALIZE_CHAR){
        columnType = new ColumnKey(name.toString());
      } else if (typeOfColumn == RowTombstoneKey.SERIALIZE_CHAR){
        columnType = new RowTombstoneKey();
      } else {
        throw new RuntimeException("can not handle "+typeOfColumn);
      }
      int size = readTwoByteSize(bg);
      char typeOfValue = (char) bg.dst[bg.currentIndex];
      bg.advanceIndex();
      AtomValue atomValue = null;
      if (typeOfValue == 'C'){
        int soFar = 0;
        StringBuilder create = endColumnPart(bg);
        soFar += create.length();
        bg.advanceIndex();
        StringBuilder time = endColumnPart(bg);
        soFar += time.length();
        bg.advanceIndex();
        StringBuilder ttl = endColumnPart(bg);
        soFar += ttl.length();
        bg.advanceIndex();
        soFar += 5;
        int remaining = size - soFar;
        bg.advanceIndex();
        StringBuilder value = readNextNIntoBuilder(bg, remaining);
        ColumnValue v = new ColumnValue();
        v.setValue(value.toString());
        v.setTime(Long.parseLong(time.toString()));
        v.setTtl(Long.parseLong(ttl.toString()));
        v.setCreateTime(Long.parseLong(create.toString()));
        atomValue = v;
      } else if (typeOfValue == 'T'){
        StringBuilder value = readNextNIntoBuilder(bg, size-1);
        TombstoneValue tv = new TombstoneValue(Long.parseLong(value.toString()));
        atomValue = tv;
      } else {
        throw new RuntimeException("can not handle "+typeOfValue);
      }
      result.put(columnType, atomValue);
    }
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
          TombstoneValue v = (TombstoneValue) columns.get(new RowTombstoneKey());
          if (v == null){
            return columns.get(new ColumnKey(column));
          } else {
            AtomValue atomValue = columns.get(new ColumnKey(column));
            if (atomValue.getTime() > v.getTime()){
              return atomValue;
            } else {
              return v;
            }
          }
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
