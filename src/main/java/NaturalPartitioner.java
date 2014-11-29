
public class NaturalPartitioner implements Partitioner {
  public Token partition(String in){
    Token t = new Token();
    t.setRowkey(in);
    t.setToken(in);
    return t; 
  }
}
