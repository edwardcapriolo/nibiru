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
    conf.setDataDirectory(testFolder.getRoot().getPath());
    XmlStorage x = new XmlStorage();
    Map<String,KeyspaceAndStoreMetaData> m = new HashMap<>();
    KeyspaceMetaData key = new KeyspaceMetaData();
    key.setName("abc");
    StoreMetaData cf = new StoreMetaData();
    cf.setName("def");
    KeyspaceAndStoreMetaData k = new KeyspaceAndStoreMetaData();
    k.setKeyspaceMetaData(key);
    k.getColumnFamilies().put("def", cf);
    m.put("abc", k);
    x.persist(conf, m);
    Map<String,KeyspaceAndStoreMetaData> result = x.read(conf);
    Assert.assertEquals("abc", result.get("abc").getKeyspaceMetaData().getName());
    //Assert.assertEquals(1, result.get("abc").getColumnFamilyMetaData().size());
    //Assert.assertEquals("def", result.get("abc").getColumnFamilyMetaData().get("def").getName());
  }
  
}
