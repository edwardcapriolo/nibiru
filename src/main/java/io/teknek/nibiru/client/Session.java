package io.teknek.nibiru.client;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;

public class Session {
  private final ColumnFamilyClient client;
  private final String keyspace;
  private final String columnFamily;
  private final Consistency writeConsistency;
  private final Consistency readConsistency;
  private final long timeoutMillis;
  private ObjectMapper MAPPER = new ObjectMapper();
  
  Session(ColumnFamilyClient client, String keyspace, String columnFamily, Consistency writeConsistency, Consistency readConsistency, long timeoutMillis){
    this.client = client;
    this.keyspace = keyspace;
    this.columnFamily = columnFamily;
    this.writeConsistency = writeConsistency;
    this.readConsistency = readConsistency;
    this.timeoutMillis = timeoutMillis;
  }
  
  public Val get(String rowkey, String column) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "get")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("timeout", timeoutMillis)
            .put("consistency", readConsistency)
            .build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
      return MAPPER.convertValue(response.get("payload"), Val.class);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
 
  public void delete(String rowkey, String column, long time) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "delete")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("timeout", timeoutMillis)
            .put("consistency", writeConsistency)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public void put(String rowkey, String column, String value, long time, long ttl) throws ClientException{
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
            .put("timeout", timeoutMillis)
            .put("consistency", writeConsistency)
            .put("ttl", ttl).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public Response put(String rowkey, String column, String value, long time) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setColumnFamily(columnFamily);
    m.setRequestPersonality(ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("value", value)
            .put("timeout", timeoutMillis)
            .put("consistency", writeConsistency)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      return client.post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
}