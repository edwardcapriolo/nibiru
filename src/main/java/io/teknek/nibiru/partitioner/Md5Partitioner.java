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
