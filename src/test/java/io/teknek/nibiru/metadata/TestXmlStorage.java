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
    conf.setDataDirectory(testFolder.getRoot());
    XmlStorage x = new XmlStorage();
    Map<String,KeyspaceAndColumnFamilyMetaData> m = new HashMap<>();
    KeyspaceMetaData key = new KeyspaceMetaData();
    key.setName("abc");
    ColumnFamilyMetaData cf = new ColumnFamilyMetaData();
    cf.setName("def");
    KeyspaceAndColumnFamilyMetaData k = new KeyspaceAndColumnFamilyMetaData();
    k.setKeyspaceMetaData(key);
    k.getColumnFamilies().put("def", cf);
    m.put("abc", k);
    x.persist(conf, m);
    Map<String,KeyspaceAndColumnFamilyMetaData> result = x.read(conf);
    Assert.assertEquals("abc", result.get("abc").getKeyspaceMetaData().getName());
    //Assert.assertEquals(1, result.get("abc").getColumnFamilyMetaData().size());
    //Assert.assertEquals("def", result.get("abc").getColumnFamilyMetaData().get("def").getName());
  }
  
}
