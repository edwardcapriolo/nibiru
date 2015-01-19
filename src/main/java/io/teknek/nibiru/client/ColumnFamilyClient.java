/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru.client;

import java.io.IOException;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableMap;

import io.teknek.nibiru.Consistency;
import io.teknek.nibiru.ConsistencyLevel;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class ColumnFamilyClient extends Client {

  public ColumnFamilyClient(String host, int port) {
    super(host, port);
  }

  public SessionBuilder createBuilder(){
    return new SessionBuilder(this);
  }
}

class SessionBuilder {
  private final ColumnFamilyClient client;
  private String keyspace;
  private String columnFamily;
  private Consistency writeConsistency;
  private Consistency readConsistency;
  private long timeoutMillis;
  
  SessionBuilder(ColumnFamilyClient client){
    this.client = client;
    //writeConsistency = new Consistency().withLevel(ConsistencyLevel.N).withParamater("must-ack", 1);
    writeConsistency = new Consistency().withLevel(ConsistencyLevel.IMPLIED);
    readConsistency = new Consistency().withLevel(ConsistencyLevel.IMPLIED);
    timeoutMillis = 10000;
  }
  
  public SessionBuilder withKeyspace(String keyspace){
    this.keyspace = keyspace;
    return this;
  }
  
  public SessionBuilder withColumnFamily(String columnFamily){
    this.columnFamily = columnFamily;
    return this;
  }
    
  public SessionBuilder withWriteConsistency(ConsistencyLevel level, Map<String,Object> parameters){
    writeConsistency = new Consistency().withLevel(level).withParameters(parameters);
    return this;
  }
  
  public SessionBuilder withReadConsistency(ConsistencyLevel level, Map<String,Object> parameters){
    readConsistency = new Consistency().withLevel(level).withParameters(parameters);
    return this;
  }
  
  public Session build(){
    return new Session(client, keyspace, columnFamily, writeConsistency, readConsistency, timeoutMillis);
  }
}

class Session {
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

  public void put(String rowkey, String column, String value, long time) throws ClientException{
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
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      throw new ClientException(e);
    }
  }
  
}

