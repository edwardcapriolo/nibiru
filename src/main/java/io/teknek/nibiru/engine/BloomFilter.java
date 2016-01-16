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

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Token;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

public class BloomFilter {
  
  private com.google.common.hash.BloomFilter<Token> bloomFilter;
  
  public BloomFilter(){
    
  }

  public void open(String id, Configuration configuration) throws IOException {
    BufferedInputStream bi = new BufferedInputStream(new FileInputStream(
            BloomFilterWriter.getFileForId(id, configuration)));
    bloomFilter = com.google.common.hash.BloomFilter.readFrom(bi, 
            BloomFilterWriter.TokenFunnel.INSTANCE);
    bi.close();
  }
  
  public boolean mightContain(Token t){
    return bloomFilter.mightContain(t);
  }
}
