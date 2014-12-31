package io.teknek.nibiru.engine;

import io.teknek.nibiru.Token;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.io.BufferGroup;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.SortedMap;

public class CommitLogReader {

  private RandomAccessFile ssRaf;
  private FileChannel ssChannel;
  private MappedByteBuffer ssBuffer;
  private BufferGroup bg;
  private final String id;
  private final DefaultColumnFamily columnFamily;
  
  public CommitLogReader(String id, DefaultColumnFamily columnFamily){
    this.id = id;
    this.columnFamily = columnFamily;
  }
  
  public void open() throws IOException {
    File sstable = new File(CommitLog.getCommitLogDirectoryForColumnFamily(columnFamily), id + "." + CommitLog.EXTENSION);
    ssRaf = new RandomAccessFile(sstable, "r");
    ssChannel = ssRaf.getChannel();
    ssBuffer = ssChannel.map(FileChannel.MapMode.READ_ONLY, 0, ssChannel.size());
    bg = new BufferGroup();
    bg.channel = ssChannel; 
    bg.mbb = (MappedByteBuffer) ssBuffer.duplicate();
    bg.setStartOffset((int) 0);
  }
  
  public Token getNextToken() throws IOException {
    if (! (bg.currentIndex < bg.dst.length - 1 || bg.mbb.position()  < bg.channel.size())){
      return null;
    }
    if (bg.dst[bg.currentIndex] == SsTableReader.END_ROW){
      bg.advanceIndex();
    }
    SsTableReader.readHeader(bg); 
    StringBuilder token = SsTableReader.readToken(bg);
    StringBuilder rowkey = SsTableReader.readRowkey(bg);
    Token t = new Token();
    t.setRowkey(rowkey.toString());
    t.setToken(token.toString());
    return t;
  }
  
  public SortedMap<String,Val>  readColumns() throws IOException {
    SortedMap<String,Val> columns = SsTableReader.readColumns(bg);
    return columns;
  }
  
}
