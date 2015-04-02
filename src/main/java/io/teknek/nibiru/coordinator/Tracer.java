package io.teknek.nibiru.coordinator;

import org.apache.log4j.Logger;

import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.HttpJsonTransport;
import io.teknek.nibiru.transport.Message;

public class Tracer {
  
  private static Logger LOGGER = Logger.getLogger(HttpJsonTransport.class);

  public static final String TRACE_PROP = "trace";
  
  public boolean shouldTrace(Message message){
    if (message.getPayload() == null){
      return false;
    }
    return message.getPayload().containsKey(TRACE_PROP);
  }
  
  public void trace(Message message, String s, Object ... stuff){
    TraceTo t = (TraceTo) message.getPayload().get(TRACE_PROP);
    if (t == TraceTo.LOGGER){
      LOGGER.info(System.currentTimeMillis() + " " + String.format(s, stuff) );
    }
  }
}
