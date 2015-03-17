package io.teknek.nibiru.cli;

import io.teknek.nibiru.client.ClientException;
import io.teknek.nibiru.client.MetaDataClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Cli {

  public static void main (String [] args) throws IOException{
    MetaDataClient meta = null;
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String line;
    while ((line = br.readLine()) != null){
      String [] parts = line.split("\\s+");
      if  ("connect".equalsIgnoreCase(parts[0])){
        meta = new MetaDataClient(parts[1],Integer.parseInt(parts[2]));
      } else if  ("showcluster".equalsIgnoreCase(parts[0])){
        try {
          System.out.println(meta.getLiveMembers());
        } catch (ClientException e) {
          System.out.println(e.getMessage());
        }
      } else if ("exit".equalsIgnoreCase(parts[0])){
        break;
      } else {
        System.out.println("no command for "+ parts[0]);
      }
      System.out.println("ok>");
    }
  }
}
