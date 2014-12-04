package io.teknek.nibiru;

import java.io.File;

public class Configuration {
  private File sstableDirectory;
  public Configuration(){
    
  }
  public File getSstableDirectory() {
    return sstableDirectory;
  }
  public void setSstableDirectory(File sstableDirectory) {
    this.sstableDirectory = sstableDirectory;
  }

}
