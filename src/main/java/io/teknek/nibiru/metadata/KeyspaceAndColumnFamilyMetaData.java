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

import java.util.HashMap;
import java.util.Map;

/**
 * This class is to persiste and because for the server structure we do not want
 * KeyspaceMetaData to has-a columnFamilyMetaData. 
 * @author edward
 *
 */
public class KeyspaceAndColumnFamilyMetaData {
  private KeyspaceMetaData keyspaceMetaData;
  private Map<String,ColumnFamilyMetaData> columnFamilies;
  
  public KeyspaceAndColumnFamilyMetaData(){
    columnFamilies = new HashMap<>();
  }

  public KeyspaceMetaData getKeyspaceMetaData() {
    return keyspaceMetaData;
  }

  public void setKeyspaceMetaData(KeyspaceMetaData keyspaceMetaData) {
    this.keyspaceMetaData = keyspaceMetaData;
  }

  public Map<String, ColumnFamilyMetaData> getColumnFamilies() {
    return columnFamilies;
  }

  public void setColumnFamilies(Map<String, ColumnFamilyMetaData> columnFamilies) {
    this.columnFamilies = columnFamilies;
  }

  @Override
  public String toString() {
    return "KeyspaceAndColumnFamilyMetaData [keyspaceMetaData=" + keyspaceMetaData
            + ", columnFamilies=" + columnFamilies + "]";
  }
  
}
