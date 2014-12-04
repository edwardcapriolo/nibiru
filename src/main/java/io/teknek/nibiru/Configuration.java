package io.teknek.nibiru;

public class Configuration {
  private String sstableDirectory;
  public Configuration(){
    
  }
  public String getSstableDirectory() {
    return sstableDirectory;
  }
  public void setSstableDirectory(String sstableDirectory) {
    this.sstableDirectory = sstableDirectory;
  }
  
}
