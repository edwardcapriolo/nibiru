package io.teknek.nibiru.metadata;

import io.teknek.nibiru.Configuration;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Map;

public class XmlStorage implements MetaDataStorage {

  public static final String SCHEMA_NAME = "schema.xml";
  
  @Override
  public void persist(Configuration configuration, Map<String, KeyspaceAndColumnFamilyMetaData> writeThis) {
    XMLEncoder e ;
    try {
      
      e = new XMLEncoder (new BufferedOutputStream(new FileOutputStream(
              new File(configuration.getDataDirectory(), SCHEMA_NAME))));
    } catch (FileNotFoundException ex){
      throw new RuntimeException("im dead", ex);
    }
    e.writeObject(writeThis);
    e.close();
    
  }

  @Override
  public Map<String, KeyspaceAndColumnFamilyMetaData> read(Configuration configuration) {
    XMLDecoder d;
    try {
      d = new XMLDecoder(
              new BufferedInputStream(
                  new FileInputStream(new File(configuration.getDataDirectory(), SCHEMA_NAME))));
    } catch (FileNotFoundException e) {
      return null;
    }
    Object result = d.readObject();
    d.close();
    return (Map<String, KeyspaceAndColumnFamilyMetaData>) result;
  }

}
