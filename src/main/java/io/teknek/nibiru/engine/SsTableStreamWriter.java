package io.teknek.nibiru.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.io.CountingBufferedOutputStream;

public class SsTableStreamWriter {

  private final ColumnFamily columnFamily;
  private final String id;
  private final IndexWriter indexWriter;
  private CountingBufferedOutputStream ssOutputStream;
  private BloomFilterWriter bloomFilter;
  
  public SsTableStreamWriter(String id, ColumnFamily columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
    indexWriter = new IndexWriter(id, columnFamily);
    bloomFilter = new BloomFilterWriter(id, columnFamily.getKeyspace().getConfiguration());
  }
  
  public void open() throws FileNotFoundException {
    File sstableFile = new File(columnFamily.getKeyspace().getConfiguration().getDataDirectory(), id + ".ss");
    if (!columnFamily.getKeyspace().getConfiguration().getDataDirectory().exists()){
      boolean create = columnFamily.getKeyspace().getConfiguration().getDataDirectory().mkdirs();
      if (!create){
        throw new RuntimeException ("could not create "+ columnFamily.getKeyspace().getConfiguration().getDataDirectory());
      }
    }
    ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
    indexWriter.open();
  }
  
  public void write(Token t, Map<String,Val> columns) throws IOException {
    long startOfRecord = ssOutputStream.getWrittenOffset();
    bloomFilter.put(t);
    ssOutputStream.writeAndCount(SsTableReader.START_RECORD);
    ssOutputStream.writeAndCount(t.getToken().getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_TOKEN);
    ssOutputStream.writeAndCount(t.getRowkey().getBytes());
    ssOutputStream.writeAndCount(SsTableReader.END_ROWKEY);
    indexWriter.handleRow(startOfRecord, t.getToken());
    boolean writeJoin = false;
    for (Entry<String, Val> j : columns.entrySet()){
      if (!writeJoin){
        writeJoin = true;
      } else {
        ssOutputStream.writeAndCount(SsTableReader.END_COLUMN);
      }
      ssOutputStream.writeAndCount(j.getKey().getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getCreateTime()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTime()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getTtl()).getBytes());
      ssOutputStream.writeAndCount(SsTableReader.END_COLUMN_PART);
      ssOutputStream.writeAndCount(String.valueOf(j.getValue().getValue()).getBytes());
    }
    ssOutputStream.writeAndCount(SsTableReader.END_ROW);
  }
  
  public void close() throws IOException {
    indexWriter.close();
    ssOutputStream.close();
    bloomFilter.writeAndClose();
  }
}
