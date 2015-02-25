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

import io.teknek.nibiru.io.BufferGroup;

import java.io.IOException;

public class IndexReader {

  public static final char END_TOKEN = '\1';
  private final BufferGroup bgIndex;

  public IndexReader(BufferGroup bgIndex) {
    this.bgIndex = bgIndex;
  }

  public long findStartOffset(String token) throws IOException {
    long offset = 0;
    do {
      if (bgIndex.dst[bgIndex.currentIndex] == SsTableReader.END_ROW) {
        bgIndex.advanceIndex();
      }
      readHeader(bgIndex);
      StringBuilder readToken = readToken(bgIndex);
      long thisOffset = readIndexSize(bgIndex);
      if(readToken.toString().equals(token)){
        return thisOffset;
      } else if (readToken.toString().compareTo(token) > 0) {
        return offset;
      } else {
        offset = thisOffset;
      }
    } while ( bgIndex.currentIndex < bgIndex.dst.length - 1 || bgIndex.mbb.position()  < bgIndex.channel.size());
    return offset;
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
  
  private Long readIndexSize(BufferGroup bg) throws IOException {
    StringBuilder token = new StringBuilder();
    while (bg.dst[bg.currentIndex] != SsTableReader.END_ROW){
      token.append((char) bg.dst[bg.currentIndex]);
      bg.advanceIndex();
    }
    return Long.valueOf(token.toString());
  }
}
