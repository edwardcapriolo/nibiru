package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MajorityValueResultMerger implements ResultMerger {

  @Override
  public Response merge(List<Response> responses, Message message) {
    Map<String,Integer> count = new HashMap<String,Integer>();
    for (Response r : responses){
      String result = (String) r.get("payload");
      if (count.containsKey(result)){
        count.put(result, count.get(result) + 1);
      } else {
        count.put(result, 1);
      }
    }
    int highest = 0;
    String value = "";
    for (Map.Entry<String, Integer> e : count.entrySet()){
      if (e.getValue() > highest){
        highest = e.getValue();
        value = e.getKey();
      } 
    }
    return new Response().withProperty("payload", value);
  }

}