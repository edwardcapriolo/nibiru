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
    bloomFilter = com.google.common.hash.BloomFilter.readFrom(bi, BloomFilterWriter.TokenFunnel.INSTANCE);
    bi.close();
  }
  
  public boolean mightContain(Token t){
    return bloomFilter.mightContain(t);
  }
}
