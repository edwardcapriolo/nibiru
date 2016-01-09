package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Val;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamily.DeleteMessage;
import io.teknek.nibiru.transport.columnfamily.GetMessage;
import io.teknek.nibiru.transport.columnfamily.PutMessage;

import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

public class HighestTimestampResultMerger implements ResultMerger {

  private static ObjectMapper OM = new ObjectMapper();
  
  @Override
  public Response merge(List<Response> responses, BaseMessage message) {
    if (message instanceof PutMessage ||  message instanceof DeleteMessage) {
      return new Response();
    } else if (message instanceof GetMessage) {
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
