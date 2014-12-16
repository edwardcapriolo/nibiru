package io.teknek.nibiru.engine;

import io.teknek.nibiru.Configuration;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

public class SSTableWriter {

  public void flushToDisk(String id, Configuration conf, Memtable m) throws IOException{
    File sstableFile = new File(conf.getSstableDirectory(), id + ".ss");
    CountingBufferedOutputStream ssOutputStream = null;
    IndexWriter indexWriter = new IndexWriter(id, conf);
    try {
      ssOutputStream = new CountingBufferedOutputStream(new FileOutputStream(sstableFile));
      indexWriter.open();
      for (Entry<Token, ConcurrentSkipListMap<String, Val>> i : m.getData().entrySet()){
        long startOfRecord = ssOutputStream.getWrittenOffset();
        ssOutputStream.writeAndCount(SsTableReader.START_RECORD);
        ssOutputStream.writeAndCount(i.getKey().getToken().getBytes());
        ssOutputStream.writeAndCount(SsTableReader.END_TOKEN);
        ssOutputStream.writeAndCount(i.getKey().getRowkey().getBytes());
        ssOutputStream.writeAndCount(SsTableReader.END_ROWKEY);
        indexWriter.handleRow(startOfRecord, i.getKey().getToken());
        boolean writeJoin = false;
        for (Entry<String, Val> j : i.getValue().entrySet()){
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
        ssOutputStream.writeAndCount('\n');
      }
    }
    finally {
      ssOutputStream.close();
      indexWriter.close();
    }
  }
  
  
}
