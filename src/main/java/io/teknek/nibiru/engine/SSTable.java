package io.teknek.nibiru.engine;


import io.teknek.nibiru.Configuration;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTable {

  char START_RECORD = '\0';
  private RandomAccessFile raf;
  private FileChannel channel;
  public SSTable(){
   
  }
  
  public void open(String id, Configuration conf) throws IOException {
    //File sstable = new File(conf.getSstableDirectory(), id + ".ss");
    File sstable = new File("/home/edward/something" + ".ss");
    raf = new RandomAccessFile(sstable, "r");
    channel = raf.getChannel();
  }
  
  public static class BufferGroup {
    private int blockSize = 1024;
    byte [] dst = new byte [blockSize];
    int startOffset = 0;
    int currentIndex = 0;
    private FileChannel channel;
    private MappedByteBuffer mbb;
    
    public BufferGroup(){} 
    
    private void read() throws IOException{
      if (channel.size() - startOffset < blockSize){
        blockSize = (int) (channel.size() - startOffset);
        dst = new byte[blockSize];
      }
      mbb.get(dst, startOffset, blockSize);
      currentIndex = 0;
    }
    
    private void advanceIndex() throws IOException{
      currentIndex++;
      if (currentIndex == blockSize){
        read();
      }
    }
  }

  private void readHeader(BufferGroup bg) throws IOException {
    if (bg.dst[bg.currentIndex] != '\0'){
      throw new RuntimeException("corrupt expected \0 got " + bg.dst[bg.currentIndex]  );
    }
    bg.advanceIndex();
  }
  
  private StringBuilder readToken(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    while (bg.dst[bg.currentIndex] != '\1'){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return token;
  }
  
  
  
  public Val get (String row, String column) throws IOException{
    BufferGroup bg = new BufferGroup();
    bg.channel = this.channel;
    bg.mbb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    bg.read();
    do {
      readHeader(bg);
      StringBuilder token = readToken(bg);
      System.out.println(token);
    } while (bg.startOffset < channel.size());
    return null;
  }
 
  /*
   *  public Val get (String row, String column) throws IOException{
    int blockSize = 1024;
    byte [] dst = new byte [blockSize];
    int startOffset = 0;
    StringBuilder startToken = null; 
    StringBuilder startRow = null;
    
    while (startOffset < channel.size()){
      if (channel.size() - startOffset < blockSize){
        blockSize = (int) (channel.size() - startOffset);
        dst = new byte[blockSize];
      }
      mbb.get(dst, startOffset, blockSize);
      if (startToken == null && dst[0] != '\0'){
        throw new RuntimeException("Corrupt");
      } else {
        startToken = new StringBuilder();
        startRow = new StringBuilder();
      }
      for (int j = 1; j < 1024 && dst[j] != '\1'; j++) {
        startToken.append((char)dst[j]);
      }
      for (int j = 1; j < 1024 && dst[j] != '\2'; j++) {
        startRow.append((char)dst[j]);
      }
      startOffset += blockSize;
    }
    return null;
  }
 
   */
  
  public void flushToDisk(Memtable m) throws IOException{
    File f = new File("/home/edward/something" + ".ss");
    OutputStream output = null;
    try {
      output = new BufferedOutputStream(new FileOutputStream(f));
      for (Entry<Token, ConcurrentSkipListMap<String, Val>> i : m.getData().entrySet()){
        output.write(START_RECORD);
        output.write(i.getKey().getToken().getBytes());
        output.write('\1');
        output.write(i.getKey().getRowkey().getBytes());
        output.write('\2');
        for (Entry<String, Val> j : i.getValue().entrySet()){
          
          output.write(String.valueOf(j.getValue().getCreateTime()).getBytes());
          output.write('\3');
          output.write(String.valueOf(j.getValue().getTime()).getBytes());
          output.write('\3');
          output.write(String.valueOf(j.getValue().getTtl()).getBytes());
          output.write('\3');
          output.write(String.valueOf(j.getValue().getValue()).getBytes());
        }
      }
      output.write('\n');
    }
    finally {
      output.close();
    }
    
  }
  
  public void doIt() throws IOException{
    File abc = new File("/home/edward/abc.txt");
    RandomAccessFile raf = new RandomAccessFile(abc, "r");
    FileChannel channel = raf.getChannel();
    MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
    byte [] b = new byte [8];
    mbb.get(b, 0, 2);
    System.out.println(b[0]);
    mbb.get(b, 0, 2);
    System.out.println(b[0]);
    raf.skipBytes(2);
    mbb.get(b, 0, 2);
    System.out.println(b[0]);
    
    File f = new File("/home/edward/out1.txt");
      OutputStream output = null;
      try {
        output = new BufferedOutputStream(new FileOutputStream(f));
        //output.write(aInput);
        output.write("yo".getBytes());
        output.write("\n".getBytes());
        output.write("abc".getBytes());
        
      }
      finally {
        output.close();
      }
  }
  
  public static void main (String [] args) throws IOException{
    SSTable n = new SSTable();
    n.doIt();
  }
  
  
}
