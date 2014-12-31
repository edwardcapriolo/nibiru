package io.teknek.nibiru.partitioner;

import io.teknek.nibiru.Token;

import java.security.NoSuchAlgorithmException;


public class Md5Partitioner implements Partitioner {

  private java.security.MessageDigest md; 
          
  public Md5Partitioner(){
    try {
      md = java.security.MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("DIE!");
    }
  } 
  
  public String MD5(String md5) {
    byte[] array = md.digest(md5.getBytes());
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < array.length; ++i) {
      sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100).substring(1, 3));
    }
    return sb.toString();
  }
  
  @Override
  public Token partition(String in) { 
    Token t = new Token();
    t.setRowkey(in);
    t.setToken(MD5(in));
    return t;
  }

}
