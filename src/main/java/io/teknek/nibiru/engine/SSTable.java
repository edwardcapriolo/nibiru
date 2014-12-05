package io.teknek.nibiru.engine;


import io.teknek.nibiru.Configuration;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {

  char START_RECORD = '\0';
  char END_TOKEN = '\1';
  char END_ROWKEY = '\2';
  char END_COLUMN_PART = '\3';
  char END_COLUMN = '\4';
  char END_ROW = '\n';
  private RandomAccessFile raf;
  private FileChannel channel;
  
  public SSTable(){
   
  }
  
  public void open(String id, Configuration conf) throws IOException {
    File sstable = new File(conf.getSstableDirectory(), id + ".ss");
    //File sstable = new File("/home/edward/something" + ".ss");
    raf = new RandomAccessFile(sstable, "r");
    channel = raf.getChannel();
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
  
  public Val get (String row, String column) throws IOException{
    BufferGroup bg = new BufferGroup();
    bg.channel = this.channel;
    bg.mbb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    bg.read();
    do {
      if (bg.dst[bg.currentIndex] == END_ROW){
        bg.advanceIndex();
      }
      readHeader(bg);
      StringBuilder token = readToken(bg);
      StringBuilder rowkey = readRowkey(bg);
      SortedMap<String,Val> columns = readColumns(bg);
      if (rowkey.toString().equals(row)){
        return columns.get(column);
      }
    } while (bg.startOffset + bg.currentIndex +1 < channel.size());
    return null;
  }
  
  public void flushToDisk(String id, Configuration conf, Memtable m) throws IOException{
    File f = new File(conf.getSstableDirectory(), id + ".ss");
    OutputStream output = null;
    try {
      output = new BufferedOutputStream(new FileOutputStream(f));
      for (Entry<Token, ConcurrentSkipListMap<String, Val>> i : m.getData().entrySet()){
        output.write(START_RECORD);
        output.write(i.getKey().getToken().getBytes());
        output.write(END_TOKEN);
        output.write(i.getKey().getRowkey().getBytes());
        output.write(END_ROWKEY);
        boolean writeJoin = false;
        for (Entry<String, Val> j : i.getValue().entrySet()){
          if (!writeJoin){
            writeJoin = true;
          } else {
            output.write(END_COLUMN);
          }
          output.write(j.getKey().getBytes());
          output.write(END_COLUMN_PART);
          output.write(String.valueOf(j.getValue().getCreateTime()).getBytes());
          output.write(END_COLUMN_PART);
          output.write(String.valueOf(j.getValue().getTime()).getBytes());
          output.write(END_COLUMN_PART);
          output.write(String.valueOf(j.getValue().getTtl()).getBytes());
          output.write(END_COLUMN_PART);
          output.write(String.valueOf(j.getValue().getValue()).getBytes());
        }
        output.write('\n');
      }
    }
    finally {
      output.close();
    }
  }
  
}
