package io.teknek.nibiru.client;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamily.DeleteMessage;
import io.teknek.nibiru.transport.columnfamily.GetMessage;
import io.teknek.nibiru.transport.columnfamily.PutMessage;
import io.teknek.nibiru.transport.columnfamily.SliceMessage;

import java.io.IOException;
import java.util.SortedMap;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

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
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setColumn(column);
    m.setRow(rowkey); 
    m.setConsistency(readConsistency);
    m.setTimeout((long)client.getSocketTimeoutMillis());
    m.setTraceTo(traceTo);
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
    SliceMessage m = new SliceMessage();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setRow(rowkey);
    m.setStart(start);
    m.setEnd(end);
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
    DeleteMessage m = new DeleteMessage();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setRow(rowkey);
    m.setColumn(column);
    m.setConsistency(this.writeConsistency);
    m.setVersion(time);
    m.setTimeout((long) client.getSocketTimeoutMillis());
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
    PutMessage m = new PutMessage();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setRow(rowkey);
    m.setColumn(column);
    m.setValue(value);
    m.setTimeout((long) client.getSocketTimeoutMillis());
    m.setConsistency(writeConsistency);
    m.setTtl(ttl);
    m.setVersion(time);
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
    PutMessage m = new PutMessage();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setRow(rowkey);
    m.setColumn(column);
    m.setValue(value);
    m.setTimeout((long) client.getSocketTimeoutMillis());
    m.setConsistency(this.writeConsistency);
    m.setVersion(time);
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