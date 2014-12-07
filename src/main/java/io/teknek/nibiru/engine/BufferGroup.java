package io.teknek.nibiru.engine;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BufferGroup {
  private int blockSize = 1024;
  byte [] dst = new byte [blockSize];
  int currentIndex = 0;
  FileChannel channel;
  MappedByteBuffer mbb;
  
  public BufferGroup(){} 
  
  void setStartOffset(int offset) throws IOException{
    mbb.position(offset);
    read();
  }
  
  void read() throws IOException{
    long l = channel.size();
    if (l - mbb.position() < blockSize){
      blockSize = (int) (l -mbb.position());
      dst = new byte[blockSize];
    }
    mbb.get(dst, 0, blockSize);
    currentIndex = 0;
  }
  
  void advanceIndex() throws IOException{
    currentIndex++;
    if (currentIndex == blockSize){
      read();
    }
  }
}