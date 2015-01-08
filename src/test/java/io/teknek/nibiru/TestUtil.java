package io.teknek.nibiru;

import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.metadata.ColumnFamilyMetaData;

import java.io.File;
import java.util.Map;

import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

public class TestUtil {

  public static Map<String, Object> STANDARD_COLUMN_FAMILY = new ImmutableMap.Builder<String, Object>()
  .put(ColumnFamilyMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName())
  .build();
  public static String PETS_COLUMN_FAMILY = "pets";
  public static String DATA_KEYSPACE = "data";
  
  public static Server aBasicServer(TemporaryFolder testFolder){
    Configuration configuration = TestUtil.aBasicConfiguration(testFolder);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createKeyspace(DATA_KEYSPACE, null);
    s.getMetaDataManager().createColumnFamily(DATA_KEYSPACE, PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY);
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

}
