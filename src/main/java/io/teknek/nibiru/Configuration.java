package io.teknek.nibiru;

import io.teknek.nibiru.metadata.XmlStorage;

import java.io.File;

public class Configuration {
  private File dataDirectory;
  private File commitlogDirectory;
  private String metaDataStorageClass = XmlStorage.class.getName();
  
  public Configuration(){
  }
  
  public File getDataDirectory() {
    return dataDirectory;
  }
  
  public void setDataDirectory(File sstableDirectory) {
    this.dataDirectory = sstableDirectory;
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
