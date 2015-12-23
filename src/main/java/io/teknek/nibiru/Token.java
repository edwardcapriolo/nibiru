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
package io.teknek.nibiru;

public class Token implements Comparable<Token>{
  private String token;
  private String rowkey;
  
  public Token(){
    
  }
  
  public Token(String token,String rowkey){
    this.token = token;
    this.rowkey = rowkey;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }

  public String getRowkey() {
    return rowkey;
  }

  public void setRowkey(String rowkey) {
    this.rowkey = rowkey;
  }

  @Override
  public int hashCode() {
    return token.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Token other = (Token) obj;
    if (rowkey == null) {
      if (other.rowkey != null)
        return false;
    } else if (!rowkey.equals(other.rowkey))
      return false;
    if (token == null) {
      if (other.token != null)
        return false;
    } else if (!token.equals(other.token))
      return false;
    return true;
  }

  @Override
  public int compareTo(Token o) {
    int res = getToken().compareTo(o.getToken());
    if (res < 0){
      return -1;
    } else if (res > 0) {
      return 1;
    } else {
      return getRowkey().compareTo(o.getRowkey());
    }
  }
  
}
