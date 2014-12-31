package io.teknek.nibiru.engine;

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
  public static final char END_TOKEN = '\1';
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
  
  public SsTableReader(SsTable ssTable, KeyCache keyCache){
    this.ssTable = ssTable;
    this.keyCache = keyCache;
  }
  
  public void open(String id) throws IOException{
    File sstable = new File(ssTable.getColumnFamily().getKeyspace().getConfiguration().getSstableDirectory(), id + ".ss");
    ssRaf = new RandomAccessFile(sstable, "r");
    ssChannel = ssRaf.getChannel();
    ssBuffer = ssChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssChannel.size());
    
    File index = new File(ssTable.getColumnFamily().getKeyspace().getConfiguration().getSstableDirectory(), id + ".index");
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
      throw new RuntimeException("corrupt expected \\0 got " + bg.dst[bg.currentIndex]  );
    }
    bg.advanceIndex();
  }
  
  static StringBuilder readToken(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    while (bg.dst[bg.currentIndex] != END_TOKEN){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();
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

  static SortedMap<String,Val> readColumns(BufferGroup bg) throws IOException {
    SortedMap<String,Val> result = new TreeMap<>();
    do {
      if (bg.dst[bg.currentIndex] == END_COLUMN){
        bg.advanceIndex();
      }
      StringBuilder name = readColumn(bg);
      StringBuilder create = readColumn(bg);
      StringBuilder time = readColumn(bg);
      StringBuilder ttl = readColumn(bg);
      StringBuilder value = endColumn(bg);
      Val v = new Val(value.toString(),
              Long.parseLong(time.toString()),
              Long.parseLong(create.toString()),
              Long.parseLong(ttl.toString()));
      result.put(name.toString(), v);
    } while (bg.dst[bg.currentIndex] != END_ROW);
    return result;
  }
  
  private void ignoreColumns(BufferGroup bg) throws IOException {
    do {
      bg.advanceIndex();
    } while (bg.dst[bg.currentIndex] != END_ROW);
  }

  public Val get (String row, String column) throws IOException{
    BufferGroup bgIndex = new BufferGroup();
    bgIndex.channel = indexChannel;
    bgIndex.mbb = (MappedByteBuffer) indexBuffer.duplicate();
    IndexReader index = new IndexReader(bgIndex);
    
    BufferGroup bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    long startOffset = keyCache.get(row);
    if (startOffset == -1){
      startOffset = index.findStartOffset(row);
    }
    bg.setStartOffset((int)startOffset);
    String searchToken = row;//this is not correct
    do {
      if (bg.dst[bg.currentIndex] == END_ROW){
        bg.advanceIndex();
      }
      long startOfRow = bg.mbb.position() - bg.blockSize  + bg.currentIndex;
      readHeader(bg);
      StringBuilder token = readToken(bg);
      if (token.toString().equals(searchToken)){
        StringBuilder rowkey = readRowkey(bg);
        if (rowkey.toString().equals(row)){
          keyCache.put(row, startOfRow);
          SortedMap<String,Val> columns = readColumns(bg);
          return columns.get(column);
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
