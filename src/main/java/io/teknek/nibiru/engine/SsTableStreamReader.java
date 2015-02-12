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
import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.io.BufferGroup;

import java.io.IOException;
import java.util.SortedMap;

public class SsTableStreamReader {

  private BufferGroup bg;
  
  public SsTableStreamReader(BufferGroup bufferGroup){
    this.bg = bufferGroup;
  }
  
  public Token getNextToken() throws IOException {
    if (! (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < bg.channel.size())){
      return null;
    }
    if (bg.dst[bg.currentIndex] == SsTableReader.END_ROW){
      bg.advanceIndex();
    }
    SsTableReader.readHeader(bg); 
    StringBuilder token = SsTableReader.readToken(bg);
    StringBuilder rowkey = SsTableReader.readRowkey(bg);
    Token t = new Token();
    t.setRowkey(rowkey.toString());
    t.setToken(token.toString());
    return t;
  }
  
  public SortedMap<AtomKey,Val>  readColumns() throws IOException {
    SortedMap<AtomKey,Val> columns = SsTableReader.readColumns(bg);
    return columns;
  }
  
}
