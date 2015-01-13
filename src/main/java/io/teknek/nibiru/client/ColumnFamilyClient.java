package io.teknek.nibiru.client;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;

import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

//TODO address concepts of consistency level
public class ColumnFamilyClient extends Client {

  private static ObjectMapper MAPPER = new ObjectMapper();
  
  public ColumnFamilyClient(String host, int port) {
    super(host, port);
  }


  public Val get(String keyspace, String columnFamily, String rowkey, String column) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "get")
            .put("rowkey", rowkey)
            .put("column", column).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
      return MAPPER.convertValue(response.get("payload"), Val.class);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public void delete(String keyspace, String columnFamily, String rowkey, String column, long time) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "delete")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public void put(String keyspace, String columnFamily, String rowkey, String column, String value, long time, long ttl) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("value", value)
            .put("time", time)
            .put("ttl", ttl).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public void put(String keyspace, String columnFamily,String rowkey, String column, String value, long time) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("value", value)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      Response response = post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
}
