package io.teknek.nibiru.router;

public class TokenId implements Comparable<TokenId> {

  private String token;
  private String id;
  
  public TokenId(){
    
  }
  
  public String getToken() {
    return token;
  }
  public void setToken(String token) {
    this.token = token;
  }
  
  public String getId() {
    return id;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  @Override
  public int compareTo(TokenId o) {
    return token.compareToIgnoreCase(o.token);
  }
  
}

