package io.teknek.nibiru.transport;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import io.teknek.nibiru.Configuration;

import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

public class HttpJsonTransport {
 
  private Server server;
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final Configuration configuration;
  private final AtomicBoolean RUNNING = new AtomicBoolean(false);
  
  public HttpJsonTransport(Configuration configuration){
    this.configuration = configuration;
  }
  
  public void init(){
    server = new Server(configuration.getTransportPort());
    server.setHandler(getHandler());
    try {
      server.start();
      RUNNING.set(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public void shutdown() {
    try {
      server.stop();
      RUNNING.set(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Handler getHandler(){
    AbstractHandler handler = new AbstractHandler() {
      public void handle(String target, HttpServletRequest request, HttpServletResponse response,
              int dispatch) throws IOException, ServletException {
        Request baseRequest = request instanceof Request ? (Request) request : HttpConnection
                .getCurrentConnection().getRequest();
        String url = baseRequest.getRequestURI();
        Message message = MAPPER.readValue(baseRequest.getInputStream(), Message.class);
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json;charset=utf-8");
        //MAPPER.writeValue(response.getOutputStream(), copy.doRequest(requestFromBody));
        response.getOutputStream().close();
        baseRequest.setHandled(true);
      }
    };
    return handler;
  }
}
