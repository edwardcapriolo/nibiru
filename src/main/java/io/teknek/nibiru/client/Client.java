package io.teknek.nibiru.client;

import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.map.ObjectMapper;

public class Client {

  private ObjectMapper MAPPER = new ObjectMapper();
  private DefaultHttpClient httpClient = new DefaultHttpClient();
  
  public Response post(String url, Message request)
          throws IOException, IllegalStateException, UnsupportedEncodingException, RuntimeException {
    HttpPost postRequest = new HttpPost(url);
    ByteArrayEntity input = new ByteArrayEntity(MAPPER.writeValueAsBytes(request));
    input.setContentType("application/json");
    postRequest.setEntity(input);
    HttpResponse response = httpClient.execute(postRequest);
    if (response.getStatusLine().getStatusCode() != 200) {
      throw new RuntimeException("Failed : HTTP error code : "
              + response.getStatusLine().getStatusCode());
    }
    Response r = MAPPER.readValue(response.getEntity().getContent(), Response.class);
    response.getEntity().getContent().close();
    return r;
  }
  
}

