package io.teknek.nibiru;

import java.io.File;

public class Configuration {
  private File sstableDirectory;
  private int indexInterval;
  
  public Configuration(){
    indexInterval = 1000;
  }
  
  public File getSstableDirectory() {
    return sstableDirectory;
  }
  
  public void setSstableDirectory(File sstableDirectory) {
    this.sstableDirectory = sstableDirectory;
  }
  
  public int getIndexInterval() {
    return indexInterval;
  }
  
  public void setIndexInterval(int indexInterval) {
    this.indexInterval = indexInterval;
  }  
}
