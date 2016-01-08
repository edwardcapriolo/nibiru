package io.teknek.nibiru.client;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamily.GetMessage;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.google.common.collect.ImmutableMap;

public class Session {
  private final Client client;
  private final String keyspace;
  private final String store;
  private final Consistency writeConsistency;
  private final Consistency readConsistency;
  private ObjectMapper MAPPER = new ObjectMapper();
  private final TraceTo traceTo;
  
  Session(Client client, String keyspace, String store, Consistency writeConsistency, Consistency readConsistency, TraceTo traceTo){
    this.client = client;
    this.keyspace = keyspace;
    this.store = store;
    this.writeConsistency = writeConsistency;
    this.readConsistency = readConsistency;
    this.traceTo = traceTo;
  }
  
  public Val get(String rowkey, String column) throws ClientException {
    GetMessage m = new GetMessage();
    m.setColumn(column);
    m.setRow(rowkey);
    m.setConsistency(readConsistency);
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setTimeout((long)client.getSocketTimeoutMillis());
    m.setTraceTo(traceTo);
    /*
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
    Response payload = new Response()
            .withProperty("type", "get")
            .withProperty("rowkey", rowkey)
            .withProperty("column", column)
            .withProperty("timeout", client.getSocketTimeoutMillis())
            .withProperty("consistency", readConsistency);
    if (traceTo != null){
      payload.withProperty(Tracer.TRACE_PROP, this.traceTo);
    }
    m.setPayload(payload);
    */
    try {
      Response response = client.post(m);
      if (response == null){
        throw new ClientException("Protocol error: response was null");
      }
      if (response.containsKey("exception")){
        throw new ClientException((String) response.get("exception"));
      }
      return MAPPER.convertValue(response.get("payload"), Val.class);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
  public SortedMap<String,Val> slice(String rowkey, String start, String end) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "slice")
            .put("rowkey", rowkey)
            .put("start", start)
            .put("end", end)
            .put("timeout", client.getSocketTimeoutMillis())
            .put("consistency", readConsistency)
            .build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
      if (response == null){
        throw new ClientException("Protocol error: response was null");
      }
      if (response.containsKey("exception")){
        throw new ClientException((String) response.get("exception"));
      }
      TypeReference<SortedMap<String,Val>> tr = new TypeReference<SortedMap<String,Val>>(){};
      return MAPPER.convertValue(response.get("payload"), tr);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
 
  public Response delete(String rowkey, String column, long time) throws ClientException {
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "delete")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("timeout", client.getSocketTimeoutMillis())
            .put("consistency", writeConsistency)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
      if (response == null){
        throw new ClientException("Protocol error: response was null");
      }
      if (response.containsKey("exception")){
        throw new ClientException((String) response.get("exception"));
      } else {
        return response;
      }
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public Response put(String rowkey, String column, String value, long time, long ttl) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("value", value)
            .put("time", time)
            .put("timeout", client.getConnectionTimeoutMillis())
            .put("consistency", writeConsistency)
            .put("ttl", ttl).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
      if (response == null){
        throw new ClientException("Protocol error: response was null");
      }
      if (response.containsKey("exception")){
        throw new ClientException((String) response.get("exception"));
      } else {
        return response;
      }
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }

  public Response put(String rowkey, String column, String value, long time) throws ClientException{
    Message m = new Message();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setPersonality(ColumnFamilyPersonality.PERSONALITY);
    Map<String,Object> payload = new ImmutableMap.Builder<String, Object>()
            .put("type", "put")
            .put("rowkey", rowkey)
            .put("column", column)
            .put("value", value)
            .put("timeout", client.getSocketTimeoutMillis())
            .put("consistency", writeConsistency)
            .put("time", time).build();
    m.setPayload(payload);
    try {
      Response response = client.post(m);
      if (response == null){
        throw new ClientException("Protocol error: response was null");
      }
      if (response.containsKey("exception")){
        throw new ClientException((String) response.get("exception"));
      } else {
        return response;
      }
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
}