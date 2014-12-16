package io.teknek.nibiru.engine;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import io.teknek.nibiru.Configuration;

public class IndexWriter {

  private final String id;
  private final Configuration conf;
  private BufferedOutputStream indexStream;
  private long rowkeyCount;
  
  public IndexWriter(String id, Configuration conf){
    this.id = id;
    this.conf = conf;
  }
  
  public void open() throws FileNotFoundException {
    File indexFile = new File(conf.getSstableDirectory(), id + ".index");
    indexStream = new BufferedOutputStream(new FileOutputStream(indexFile));
  }
  
  public void handleRow(long startOfRecord, String token) throws IOException {
    if (rowkeyCount++ % conf.getIndexInterval() == 0){
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
