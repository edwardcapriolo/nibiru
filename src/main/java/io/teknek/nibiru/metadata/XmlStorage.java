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
  public void persist(Configuration configuration, Map<String, KeyspaceAndStoreMetaData> writeThis) {
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
  public Map<String, KeyspaceAndStoreMetaData> read(Configuration configuration) {
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
    return (Map<String, KeyspaceAndStoreMetaData>) result;
  }

}
