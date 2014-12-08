package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {

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
  
  public SSTable(){
   
  }
  
  public void open(String id, Configuration conf) throws IOException {
    File sstable = new File(conf.getSstableDirectory(), id + ".ss");
    ssRaf = new RandomAccessFile(sstable, "r");
    ssChannel = ssRaf.getChannel();
    ssBuffer = ssChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssChannel.size());
    
    File index = new File(conf.getSstableDirectory(), id + ".index");
    indexRaf = new RandomAccessFile(index, "r");
    indexChannel = indexRaf.getChannel();
    indexBuffer = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size());
    
  }
  
  private void readHeader(BufferGroup bg) throws IOException {
    if (bg.dst[bg.currentIndex] != '\0'){
      throw new RuntimeException("corrupt expected \\0 got " + bg.dst[bg.currentIndex]  );
    }
    bg.advanceIndex();
  }
  
  private StringBuilder readToken(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    while (bg.dst[bg.currentIndex] != END_TOKEN){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();
    return token;
  }
  
  private StringBuilder readRowkey(BufferGroup bg) throws IOException {
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
  
  private StringBuilder readColumn(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (bg.dst[bg.currentIndex] != END_COLUMN_PART){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    bg.advanceIndex();
    return create;
  }
  
  private StringBuilder endColumn(BufferGroup bg) throws IOException{
    StringBuilder create = new StringBuilder();
    while (!(bg.dst[bg.currentIndex] == END_COLUMN ||bg.dst[bg.currentIndex] == END_ROW) ){
      create.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return create;
  }

  private SortedMap<String,Val> readColumns(BufferGroup bg) throws IOException {
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
    bg.setStartOffset((int)index.findStartOffset(row));
    String searchToken = row;//this is not correct
    do {
      if (bg.dst[bg.currentIndex] == END_ROW){
        bg.advanceIndex();
      }
      readHeader(bg);
      StringBuilder token = readToken(bg);
      if (token.toString().equals(searchToken)){
        StringBuilder rowkey = readRowkey(bg);
        if (rowkey.toString().equals(row)){
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
  
  public void flushToDisk(String id, Configuration conf, Memtable m) throws IOException{
    File sstableFile = new File(conf.getSstableDirectory(), id + ".ss");
    CountingBufferedOutputStream ssOutputStream = null;
    IndexWriter indexWriter = new IndexWriter(id, conf);
    try {
      ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
      indexWriter.open();
      for (Entry<Token, ConcurrentSkipListMap<String, Val>> i : m.getData().entrySet()){
        long startOfRecord = ssOutputStream.getWrittenOffset();
        ssOutputStream.writeAndCount(START_RECORD);
        ssOutputStream.writeAndCount(i.getKey().getToken().getBytes());
        ssOutputStream.writeAndCount(END_TOKEN);
        ssOutputStream.writeAndCount(i.getKey().getRowkey().getBytes());
        ssOutputStream.writeAndCount(END_ROWKEY);
        indexWriter.handleRow(startOfRecord, i.getKey().getToken());
        boolean writeJoin = false;
        for (Entry<String, Val> j : i.getValue().entrySet()){
          if (!writeJoin){
            writeJoin = true;
          } else {
            ssOutputStream.writeAndCount(END_COLUMN);
          }
          ssOutputStream.writeAndCount(j.getKey().getBytes());
          ssOutputStream.writeAndCount(END_COLUMN_PART);
          ssOutputStream.writeAndCount(String.valueOf(j.getValue().getCreateTime()).getBytes());
          ssOutputStream.writeAndCount(END_COLUMN_PART);
          ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTime()).getBytes());
          ssOutputStream.writeAndCount(END_COLUMN_PART);
          ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTtl()).getBytes());
          ssOutputStream.writeAndCount(END_COLUMN_PART);
          ssOutputStream.writeAndCount(String.valueOf(j.getValue().getValue()).getBytes());
        }
        ssOutputStream.writeAndCount('\n');
      }
    }
    finally {
      ssOutputStream.close();
      indexWriter.close();
    }
  }
 
}
