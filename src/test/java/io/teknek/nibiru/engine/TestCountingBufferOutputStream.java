package io.teknek.nibiru.engine;

import io.teknek.nibiru.io.CountingBufferedOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCountingBufferOutputStream {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void test() throws IOException{
    File tempFolder = testFolder.newFolder("sstable");
    CountingBufferedOutputStream i = new CountingBufferedOutputStream(new FileOutputStream(new File(tempFolder, "a")));
    i.writeAndCount('a');
    Assert.assertEquals(1,i.getWrittenOffset());
    i.writeAndCount("hi".getBytes("UTF-8"));
    Assert.assertEquals(2, "hi".getBytes().length);
    Assert.assertEquals(3, i.getWrittenOffset());
    i.close();
  }
}
