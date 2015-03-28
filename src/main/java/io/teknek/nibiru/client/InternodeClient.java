package io.teknek.nibiru.client;

import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class InternodeClient {

  private Client client;
  
  public InternodeClient(String host, int port){
    client = new Client(host, port);
  }
  
  public void join(String keyspace, String sponsorId, ServerId me, String wantedToken) {
    Message m = new Message();
    m.setKeyspace("system");
    Map<String, Object> payload = new HashMap<>();
    payload.put("keyspace", keyspace);
    payload.put("sponsor_request", "");
    payload.put("request_id", me.getU().toString());
    payload.put("wanted_token", wantedToken);
    m.setPayload(payload);
    
    try {
      Response response = client.post(m);
      System.out.println(response.get("payload"));
    } catch (IOException | RuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

