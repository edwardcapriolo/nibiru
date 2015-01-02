package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Token;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public class BloomFilterWriter {

  private final BloomFilter<Token> bloomFilter;
  private static final int expectedInsertions = 100; //Be smarter here
  private final String id;
  private final Configuration configuration;
  
  public BloomFilterWriter(String id, Configuration configuration){
    this.id = id;
    this.configuration = configuration;
    bloomFilter = BloomFilter.create(TokenFunnel.INSTANCE, expectedInsertions);
  }
  
  public enum TokenFunnel implements Funnel<Token> {
    INSTANCE;
    public void funnel(Token person, PrimitiveSink into) {
      into.putUnencodedChars(person.getToken()).putUnencodedChars(person.getRowkey());
    }
  }
  
  public void put(Token t){
    bloomFilter.put(t);
  }
  
  public boolean mightContain(Token t){
    return bloomFilter.mightContain(t);
  }
  
  public static File getFileForId(String id, Configuration configuration){
    return new File(
            configuration.getSstableDirectory(), id + ".bf");
  }
  
  public void writeAndClose() throws IOException {
    BufferedOutputStream bo = new BufferedOutputStream(new FileOutputStream(getFileForId(id,configuration)));
    bloomFilter.writeTo(bo);
    bo.close();
  }
}
