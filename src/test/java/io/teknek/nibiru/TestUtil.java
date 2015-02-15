package io.teknek.nibiru;

import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.keyvalue.InMemoryKeyValue;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class TestUtil {

  public static Map<String, Object> STANDARD_COLUMN_FAMILY = new ImmutableMap.Builder<String, Object>()
  .put(ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName())
  .build();
  public static Map<String, Object> STANDARD_KEY_VLUE = new ImmutableMap.Builder<String, Object>()
          .put(ColumnFamilyMetaData.IMPLEMENTING_CLASS, InMemoryKeyValue.class.getName())
          .build();
  public static String PETS_COLUMN_FAMILY = "pets";
  public static String DATA_KEYSPACE = "data";
  public static String BOOKS_KEY_VALUE = "books";
  
  public static Server aBasicServer(TemporaryFolder testFolder){
    Configuration configuration = TestUtil.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createOrUpdateKeyspace(DATA_KEYSPACE, new HashMap<String,Object>());
    s.getMetaDataManager().createOrUpdateColumnFamily(DATA_KEYSPACE, PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY);
    s.getMetaDataManager().createOrUpdateColumnFamily(DATA_KEYSPACE, BOOKS_KEY_VALUE, TestUtil.STANDARD_KEY_VLUE);
    return s;
  }

  public static Configuration aBasicConfiguration(TemporaryFolder testFolder){
    File tempFolder = testFolder.newFolder("sstable");
    File commitlog = testFolder.newFolder("commitlog");
    Configuration configuration = new Configuration();
    configuration.setDataDirectory(tempFolder);
    configuration.setCommitlogDirectory(commitlog);
    return configuration;
  }

  public static void compareColumnValue(AtomValue v1, AtomValue v2){
    ColumnValue v3 = (ColumnValue) v1;
    ColumnValue v4 = (ColumnValue) v2;
    Assert.assertEquals(v3.getTime(), v4.getTime());
    //Assert.assertEquals(v3.getCreateTime(), v4.getCreateTime());
    Assert.assertEquals(v3.getTtl(), v4.getTtl());
    Assert.assertEquals(v3.getValue(), v4.getValue());
  }
}
