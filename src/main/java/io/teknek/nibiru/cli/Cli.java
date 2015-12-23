package io.teknek.nibiru.cli;

import io.teknek.nibiru.Val;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.client.MetaDataClient;
import io.teknek.nibiru.client.Session;
import io.teknek.nibiru.engine.DefaultColumnFamily;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Cli {

  public static void main (String [] args) throws IOException{
    MetaDataClient meta = null;
    ColumnFamilyClient client = null;
    Session session = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String line;
    System.out.println("Welcome to a very minimal Cli type 'connect <host> <port>' to get started ");
    System.out.println("ok> ");
    while ((line = br.readLine()) != null){
      String [] parts = line.split("\\s+");
      if  ("connect".equalsIgnoreCase(parts[0])){
        meta = new MetaDataClient(parts[1],Integer.parseInt(parts[2]));
        client = new ColumnFamilyClient(new Client(parts[1], Integer.parseInt(parts[2]), 10000,
                10000));
      } else if  ("showcluster".equalsIgnoreCase(parts[0])){
        try {
          System.out.println(meta.getLiveMembers());
        } catch (ClientException e) {
          System.out.println(e.getMessage());
        }
      } else if ("use".equalsIgnoreCase(parts[0])){
        session = client.createBuilder().withKeyspace(parts[1]).withStore(parts[2]).build();
      } else if ("set".equalsIgnoreCase(parts[0])) {
        try {
          session.put(parts[1], parts[2], parts[3], System.currentTimeMillis()*1000);
        } catch (ClientException e) {
          e.printStackTrace();
        }
      } else if ("get".equalsIgnoreCase(parts[0])) {
        try {
          Val v = session.get(parts[1], parts[2]);
          System.out.println(v);
        } catch (ClientException e) {
          e.printStackTrace();
        }
      }
      
      else if ("createkeyspace".equalsIgnoreCase(parts[0])) {
        
        try {
          meta.createOrUpdateKeyspace(parts[1], new HashMap<String,Object>(), true);
        } catch (ClientException e) {
          System.out.println(e);
        }
      }
      
      else if ("createcolumnfamily".equalsIgnoreCase(parts[0])) {
        try {
          Map m = new HashMap<String,Object>();
          m.put("implementing_class", DefaultColumnFamily.class.getName());
          meta.createOrUpdateStore(parts[1], parts[2], m, true);
        } catch (ClientException e) {
          System.out.println(e);
        }
      }
      
      else if ("showkeyspaces".equalsIgnoreCase(parts[0])) { 
        try {
          System.out.println(meta.listKeyspaces());
        } catch (ClientException e) {
          System.out.println(e);
        }
      }
      
      else if ("describekeyspace".equalsIgnoreCase(parts[0])) { 
        try {
          System.out.println(meta.listStores(parts[1]));
        } catch (ClientException e) {
          System.out.println(e);
        }
      }
      
      else if ("exit".equalsIgnoreCase(parts[0])){
        break;
      } else {
        System.out.println("no command for "+ parts[0]);
      }
      System.out.println("ok>");
    }
  }
}
