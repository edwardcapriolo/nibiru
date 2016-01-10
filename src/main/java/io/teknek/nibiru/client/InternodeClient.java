package io.teknek.nibiru.client;

import io.teknek.nibiru.ServerId;
import io.teknek.nibiru.Token;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomPair;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.directsstable.Close;
import io.teknek.nibiru.transport.directsstable.Open;
import io.teknek.nibiru.transport.directsstable.Write;
import io.teknek.nibiru.transport.sponsor.SponsorMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

public class InternodeClient {

  private Client client;
  
  public InternodeClient(String host, int port){
    client = new Client(host, port);
  }
  
  /**
   * 
   * @param id a uuid becomes an sstablename on server
   */
  public void createSsTable(String keyspace, String store, String id){
    Open m = new Open();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setId(id);
    try {
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  public void transmit(String keyspace, String store, Token token, SortedMap<AtomKey,AtomValue> columns, String id){
    Write w = new Write();
    w.setKeyspace(keyspace);
    w.setStore(store);
    w.setToken(token);
    List<AtomPair> p = new ArrayList<>();
    for (Entry<AtomKey, AtomValue> i : columns.entrySet()){
      p.add(new AtomPair(i.getKey(), i.getValue()));
    }
    w.setColumns(p);
    w.setId(id);
    /*
    Message m = new Message();
    m.setKeyspace("stream");
    List<AtomPair> p = new ArrayList<>();
    for (Entry<AtomKey, AtomValue> i : columns.entrySet()){
      p.add(new AtomPair(i.getKey(), i.getValue()));
    }
    m.setPersonality(DirectSsTableWriter.PERSONALITY);
    Map<String, Object> payload = new HashMap<>();
    payload.put("keyspace", keyspace);
    payload.put("type", DirectSsTableWriter.WRITE);
    payload.put("store", store);
    payload.put("token", token);
    payload.put("columns", p);
    payload.put("id", id);
    m.setPayload(payload);
    */
    try {
      Response response = client.post(w);
    } catch (IOException | RuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void closeSsTable(String keyspace, String store, String id){
    Close m = new Close();
    m.setKeyspace(keyspace);
    m.setStore(store);
    m.setId(id);
    try {
      Response response = client.post(m);
    } catch (IOException | RuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void join(String keyspace, String sponsorId, ServerId me, String wantedToken, String transportHost) {
    SponsorMessage s = new SponsorMessage();
    s.setKeyspace(keyspace);
    s.setRequestId(me.getU().toString());
    s.setWantedToken(wantedToken);
    s.setTransportHost(transportHost);
    try {
      Response response = client.post(s);
    } catch (IOException | RuntimeException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}

