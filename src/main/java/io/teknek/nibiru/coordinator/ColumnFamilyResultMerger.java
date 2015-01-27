package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Val;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

public class ColumnFamilyResultMerger implements ResultMerger {

  private static ObjectMapper OM = new ObjectMapper();
  
  @Override
  public Response merge(List<Response> responses, Message message) {
    if ("put".equals(message.getPayload().get("type"))
            || "delete".equals(message.getPayload().get("type"))) {
      return new Response();
    } else if ("get".equals(message.getPayload().get("type"))) {
      return highestTimestampResponse(responses);
    } else {
      return new Response().withProperty("exception", "unsupported operation " + message);
    }
  }
  
  private Response highestTimestampResponse(List<Response> responses){
    long highestTimestamp = Long.MIN_VALUE;
    int highestIndex = Integer.MIN_VALUE;
    for (int i = 0; i < responses.size(); i++) {
      Val v = OM.convertValue(responses.get(i).get("payload"), Val.class);
      if (v.getTime() > highestTimestamp) {
        highestTimestamp = v.getTime();
        highestIndex = i;
      }
    }
    return responses.get(highestIndex);
  }

}
