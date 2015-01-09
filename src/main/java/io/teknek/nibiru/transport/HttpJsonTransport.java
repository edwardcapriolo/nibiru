package io.teknek.nibiru.transport;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.Coordinator;

import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;

public class HttpJsonTransport {
 
  private Server server;
  private static ObjectMapper MAPPER = new ObjectMapper();
  private final Configuration configuration;
  private final AtomicBoolean RUNNING = new AtomicBoolean(false);
  private final Coordinator coordinator; 
  
  public HttpJsonTransport(Configuration configuration, Coordinator cordinator){
    this.configuration = configuration;
    this.coordinator = cordinator;
  }
  
  public void init(){
    SelectChannelConnector s = new SelectChannelConnector();
    s.setHost(configuration.getTransportHost());
    s.setPort(configuration.getTransportPort());
    server = new Server();
    server.addConnector(s);   
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
        try {
          MAPPER.writeValue(response.getOutputStream(), coordinator.handle(message));
        } catch (RuntimeException ex){
          Response r = new Response();
          r.put("exception", ex.getMessage());
          System.err.println(message);
          ex.printStackTrace();
          MAPPER.writeValue(response.getOutputStream(), r);
        }
        response.getOutputStream().close();
        baseRequest.setHandled(true);
      }
    };
    return handler;
  }
}
