package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class MajorityValueResultMerger implements ResultMerger {

  @Override
  public Response merge(List<Response> responses, BaseMessage message) {
    SortedMap<String,Integer> count = new TreeMap<String,Integer>();
    for (Response r : responses){
      String result = (String) r.get("payload");
      if (count.containsKey(result)){
        count.put(result, count.get(result) + 1);
      } else {
        count.put(result, 1);
      }
    }
    int highest = 0;
    String value = null;
    for (Map.Entry<String, Integer> e : count.entrySet()){
      if (e.getValue() > highest){
        highest = e.getValue();
        value = e.getKey();
      } 
    }
    return new Response().withProperty("payload", value);
  }

}
