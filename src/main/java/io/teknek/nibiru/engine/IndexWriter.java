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
package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import io.teknek.nibiru.Store;

public class IndexWriter {

  private final String id;
  private final Store columnFamily;
  private BufferedOutputStream indexStream;
  private long rowkeyCount;
  
  
  public IndexWriter(String id, Store columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
  }
  
  public void open() throws FileNotFoundException {
    File indexFile = new File(columnFamily.getKeyspace().getConfiguration().getDataDirectory(), id + ".index");
    indexStream = new BufferedOutputStream(new FileOutputStream(indexFile));
  }
  
  public void handleRow(long startOfRecord, String token) throws IOException {
    if (rowkeyCount++ % columnFamily.getStoreMetadata().getIndexInterval() == 0){
      indexStream.write(SsTableReader.START_RECORD);
      indexStream.write(token.getBytes());
      indexStream.write(IndexReader.END_TOKEN);
      indexStream.write(String.valueOf(startOfRecord).getBytes());
      indexStream.write(SsTableReader.END_ROW);
    }
  }
  
  public void close () throws IOException {
    indexStream.close();
  }
}
