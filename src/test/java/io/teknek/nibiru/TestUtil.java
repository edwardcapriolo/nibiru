package io.teknek.nibiru;

import io.teknek.nibiru.cluster.GossipClusterMembership;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.keyvalue.InMemoryKeyValue;
import io.teknek.nibiru.metadata.StoreMetaData;
import io.teknek.nibiru.transport.Response;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.rules.TemporaryFolder;

public class TestUtil {
  
  public static   Map<String, Object> STANDARD_COLUMN_FAMILY(){ return  new Response()
  .withProperty(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());}
  /*
  public static final Map<String, Object> STANDARD_COLUMN_FAMILY = new Response()
  .withProperty(StoreMetaData.IMPLEMENTING_CLASS, DefaultColumnFamily.class.getName());
  */

  public static final Map<String, Object> STANDARD_KEY_VLUE = new Response()
  .withProperty(StoreMetaData.IMPLEMENTING_CLASS,InMemoryKeyValue.class.getName());
  
  public static final String PETS_COLUMN_FAMILY = "pets";
  public static final String DATA_KEYSPACE = "data";
  public static final String BOOKS_KEY_VALUE = "books";
  
  public static Server aBasicServer(TemporaryFolder testFolder){
    return aBasicServer(testFolder, 7070);
  }
  
  public static Server aBasicServer(TemporaryFolder testFolder, int port){
    Configuration configuration = TestUtil.aBasicConfiguration(testFolder, port);
    Server s = new Server(configuration);
    s.init();
    s.getMetaDataManager().createOrUpdateKeyspace(DATA_KEYSPACE, new HashMap<String,Object>());
    s.getMetaDataManager().createOrUpdateStore(DATA_KEYSPACE, PETS_COLUMN_FAMILY, TestUtil.STANDARD_COLUMN_FAMILY());
    s.getMetaDataManager().createOrUpdateStore(DATA_KEYSPACE, BOOKS_KEY_VALUE, TestUtil.STANDARD_KEY_VLUE);
    return s;
  }

  public static Map<String,Object> gossipPropertiesFor127Seed(){
    Map<String, Object> clusterProperties = new HashMap<>();
    clusterProperties.put(GossipClusterMembership.HOSTS, Arrays.asList("127.0.0.1"));
    return clusterProperties;
  }
  
  public static Configuration aBasicConfiguration(TemporaryFolder testFolder){
    return aBasicConfiguration(testFolder, 7070);
  }
  
  public static Configuration aBasicConfiguration(TemporaryFolder testFolder, int port){
    File tempFolder;
    try {
      tempFolder = testFolder.newFolder("sstable");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    File commitlog;
    try {
      commitlog = testFolder.newFolder("commitlog");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Configuration configuration = new Configuration();
    configuration.setTransportPort(port);
    configuration.setDataDirectory(tempFolder.getPath());
    configuration.setCommitlogDirectory(commitlog.getPath());
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
