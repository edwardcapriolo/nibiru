package io.teknek.nibiru.engine;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BufferGroup {
  private int blockSize = 1024;
  byte [] dst = new byte [blockSize];
  int startOffset = 0;
  int currentIndex = 0;
  FileChannel channel;
  MappedByteBuffer mbb;
  
  public BufferGroup(){} 
  
  void read() throws IOException{
    if (channel.size() - startOffset < blockSize){
      blockSize = (int) (channel.size() - startOffset);
      dst = new byte[blockSize];
    }
    mbb.get(dst, startOffset, blockSize);
    currentIndex = 0;
  }
  
  void advanceIndex() throws IOException{
    currentIndex++;
    if (currentIndex == blockSize){
      read();
    }
  }
}