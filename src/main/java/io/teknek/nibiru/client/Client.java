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

import io.teknek.nibiru.transport.BaseResponse;
import io.teknek.nibiru.transport.Message;

import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.codehaus.jackson.map.ObjectMapper;

public class Client {

  protected ObjectMapper MAPPER = new ObjectMapper();
  private DefaultHttpClient client = new DefaultHttpClient();
  private ClientConnectionManager mgr;
  
  private static final int connectionTimeoutInMillis = 10000;
  private static final int socketTimeoutInMillis = 10000;
  
  private final String host;
  private final int port;
  private final int connectionTimeoutMillis;
  private final int socketTimeoutMillis;
  
  @Deprecated
  public Client(String host, int port){
    this(host, port, connectionTimeoutInMillis, socketTimeoutInMillis);
  }
  
  @SuppressWarnings("deprecation")
  public Client(String host, int port, int connectionTimeoutMillis, int socketTimeoutMillis){
    this.host = host;
    this.port = port;
    this.connectionTimeoutMillis = connectionTimeoutMillis;
    this.socketTimeoutMillis = socketTimeoutMillis;
    client = new DefaultHttpClient();
    mgr = client.getConnectionManager();
    HttpParams params = client.getParams();
    HttpConnectionParams.setConnectionTimeout(params, connectionTimeoutMillis);
    HttpConnectionParams.setSoTimeout(params, socketTimeoutMillis);
    client = new DefaultHttpClient(new ThreadSafeClientConnManager(params,
            mgr.getSchemeRegistry()), params);
  }
    
  public Response post(Message request)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {
    HttpPost postRequest = new HttpPost("http://" + host + ":" + port);
    ByteArrayEntity input = new ByteArrayEntity(MAPPER.writeValueAsBytes(request));
    input.setContentType("application/json");
    postRequest.setEntity(input);
    HttpResponse response = client.execute(postRequest);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
              + response.getStatusLine().getStatusCode());
    }
    Response r = MAPPER.readValue(response.getEntity().getContent(), Response.class);
    response.getEntity().getContent().close();
    return r;
  }
  
  public BaseResponse post(Message request, Class expectedReturn)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {
    HttpPost postRequest = new HttpPost("http://" + host + ":" + port);
    ByteArrayEntity input = new ByteArrayEntity(MAPPER.writeValueAsBytes(request));
    input.setContentType("application/json");
    postRequest.setEntity(input);
    HttpResponse response = client.execute(postRequest);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
              + response.getStatusLine().getStatusCode());
    }
    BaseResponse r = MAPPER.readValue(response.getEntity().getContent(), expectedReturn);
    response.getEntity().getContent().close();
    return r;
  }
  
  
  public void shutdown(){
    mgr.shutdown();
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public int getConnectionTimeoutMillis() {
    return connectionTimeoutMillis;
  }

  public int getSocketTimeoutMillis() {
    return socketTimeoutMillis;
  }
  
}