package io.teknek.nibiru.coordinator;

import org.apache.log4j.Logger;

import io.teknek.nibiru.TraceTo;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.HttpJsonTransport;

public class Tracer {
  
  private static Logger LOGGER = Logger.getLogger(HttpJsonTransport.class);

  public static final String TRACE_PROP = "trace"; 
  
  public boolean shouldTrace(BaseMessage message){
    /*
    if (message.getPayload() == null){
      return false;
    }
    return message.getPayload().containsKey(TRACE_PROP);*/
    return false;
  }
  
  public void trace(BaseMessage message, String s, Object ... stuff){
    /*
    TraceTo t = (TraceTo) message.getPayload().get(TRACE_PROP);
    if (t == TraceTo.LOGGER){
      LOGGER.info(System.currentTimeMillis() + " " + String.format(s, stuff) );
    }*/
  }
}
