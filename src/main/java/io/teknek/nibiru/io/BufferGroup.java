package io.teknek.nibiru.io;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BufferGroup {
  public int blockSize = 1024;
  public byte [] dst = new byte [blockSize];
  public int currentIndex = 0;
  public FileChannel channel;
  public MappedByteBuffer mbb;
  
  public BufferGroup(){} 
  
  public void setStartOffset(int offset) throws IOException{
    mbb.position(offset);
    read();
  }
  
  public void read() throws IOException{
    long l = channel.size();
    if (l - mbb.position() < blockSize){
      blockSize = (int) (l -mbb.position());
      dst = new byte[blockSize];
    }
    mbb.get(dst, 0, blockSize);
    currentIndex = 0;
  }
  
  public void advanceIndex() throws IOException{
    currentIndex++;
    if (currentIndex == blockSize){
      read();
    }
  }
  
}