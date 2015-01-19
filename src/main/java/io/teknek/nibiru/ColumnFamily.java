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
package io.teknek.nibiru;

import java.io.IOException;

import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

public abstract class ColumnFamily {
  protected final Keyspace keyspace;
  protected final ColumnFamilyMetaData columnFamilyMetadata;
  
  public ColumnFamily(Keyspace keyspace, ColumnFamilyMetaData cfmd){
    this.keyspace = keyspace;
    this.columnFamilyMetadata = cfmd;
  }

  public ColumnFamilyMetaData getColumnFamilyMetadata() {
    return columnFamilyMetadata;
  }
  
  public Keyspace getKeyspace() {
    return keyspace;
  }

  public abstract void init() throws IOException;

  public abstract void shutdown() throws IOException;

}
