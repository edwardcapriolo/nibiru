package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Configuration;

public class IndexWriter {

  private final String id;
  private final ColumnFamily columnFamily;
  private BufferedOutputStream indexStream;
  private long rowkeyCount;
  
  
  public IndexWriter(String id, ColumnFamily columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
  }
  
  public void open() throws FileNotFoundException {
    File indexFile = new File(columnFamily.getKeyspace().getConfiguration().getDataDirectory(), id + ".index");
    indexStream = new BufferedOutputStream(new FileOutputStream(indexFile));
  }
  
  public void handleRow(long startOfRecord, String token) throws IOException {
    if (rowkeyCount++ % columnFamily.getColumnFamilyMetadata().getIndexInterval() == 0){
      indexStream.write(SsTableReader.START_RECORD);
      indexStream.write(token.getBytes());
      indexStream.write(SsTableReader.END_TOKEN);
      indexStream.write(String.valueOf(startOfRecord).getBytes());
      indexStream.write(SsTableReader.END_ROW);
    }
  }
  
  public void close () throws IOException {
    indexStream.close();
  }
}
