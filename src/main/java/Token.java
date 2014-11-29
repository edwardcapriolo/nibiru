
public class Token implements Comparable<Token>{
  private String token;
  private String rowkey;
  
  public Token(){
    
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
