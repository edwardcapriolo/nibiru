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