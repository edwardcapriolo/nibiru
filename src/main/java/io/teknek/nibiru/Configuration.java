package io.teknek.nibiru;

import io.teknek.nibiru.metadata.XmlStorage;

import java.io.File;

public class Configuration {
  private File sstableDirectory;
  private int indexInterval;
  private File commitlogDirectory;
  private String metaDataStorageClass = XmlStorage.class.getName();
  
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

  public File getCommitlogDirectory() {
    return commitlogDirectory;
  }

  public void setCommitlogDirectory(File commitlogDirectory) {
    this.commitlogDirectory = commitlogDirectory;
  }

  public String getMetaDataStorageClass() {
    return metaDataStorageClass;
  }

  public void setMetaDataStorageClass(String metaDataStorageClass) {
    this.metaDataStorageClass = metaDataStorageClass;
  }  
  
}
