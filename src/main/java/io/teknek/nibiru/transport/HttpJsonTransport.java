/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.teknek.nibiru.transport;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import io.teknek.nibiru.Configuration;
import io.teknek.nibiru.coordinator.Coordinator;

import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.ServerConnector;
/*
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.nio.SelectChannelConnector;
*/
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

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
    server = new Server();
    ServerConnector s = new ServerConnector(server);
    s.setHost(configuration.getTransportHost());
    s.setPort(configuration.getTransportPort());
    server.addConnector(s);
    server.setDumpBeforeStop(true);
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
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private Handler getHandler(){

    AbstractHandler handler = new AbstractHandler() {
      /*
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
      */
      @Override
      public void handle(String target, Request request, HttpServletRequest servletRequest,
              HttpServletResponse response) throws IOException, ServletException {
        String url = request.getRequestURI();
        Message message = MAPPER.readValue(request.getInputStream(), Message.class);
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
        request.setHandled(true);
        
      }
    };
    return handler;
  }
}
