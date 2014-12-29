package io.teknek.nibiru.metadata;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import io.teknek.nibiru.Configuration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestXmlStorage {

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();
  
  @Test
  public void writeAndReadBack(){
    Configuration conf = new Configuration();
    conf.setSstableDirectory(testFolder.getRoot());
    XmlStorage x = new XmlStorage();
    Map<String,KeyspaceMetadata> m = new HashMap<>();
    KeyspaceMetadata key = new KeyspaceMetadata("abc");
    ColumnFamilyMetadata cf = new ColumnFamilyMetadata();
    cf.setName("def");
    m.put("abc", key);
    key.getColumnFamilyMetaData().put("def", cf);
    x.persist(conf, m);
    Map<String,KeyspaceMetadata> result = x.read(conf);
    Assert.assertEquals("abc", result.get("abc").getName());
    Assert.assertEquals(1, result.get("abc").getColumnFamilyMetaData().size());
    Assert.assertEquals("def", result.get("abc").getColumnFamilyMetaData().get("def").getName());
  }
  
}
