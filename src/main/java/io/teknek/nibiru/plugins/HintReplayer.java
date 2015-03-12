package io.teknek.nibiru.plugins;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.SortedMap;

import org.codehaus.jackson.map.ObjectMapper;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.Server;
import io.teknek.nibiru.client.Client;
import io.teknek.nibiru.client.ColumnFamilyClient;
import io.teknek.nibiru.cluster.ClusterMember;
import io.teknek.nibiru.coordinator.Coordinator;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.engine.atom.ColumnValue;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;

public class HintReplayer extends AbstractPlugin implements Runnable {
  
  ObjectMapper om = new ObjectMapper();

  public static final String MY_NAME = "hint_replayer";
  private volatile boolean goOn = true;
  private Thread runThread; 
  private ColumnFamilyPersonality hintCf;
  private final AtomicLong hintsDelivered = new AtomicLong();
  private final AtomicLong hintsFailed = new AtomicLong();
  private ConcurrentMap<Destination,ColumnFamilyClient> mapping;
  
  public HintReplayer(Server server) {
    super(server);
  }

  @Override
  public String getName() {
    return MY_NAME;
  }

  @Override
  public void init() {
    mapping = new ConcurrentHashMap<>();
    hintCf = Coordinator.getHintCf(server);
    runThread = new Thread(this);
    runThread.start();
  }

  @Override
  public void shutdown() {
    goOn = false;
  }

  //this is copied from eventual coordinator
  private Client clientForDestination(Destination destination){
    Client client = mapping.get(destination);
    if (client != null) {
      return client;
    }
    for (ClusterMember cm : server.getClusterMembership().getLiveMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), server.getConfiguration().getTransportPort());
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    for (ClusterMember cm : server.getClusterMembership().getDeadMembers()){
      if (cm.getId().equals(destination.getDestinationId())){
        ColumnFamilyClient cc = new ColumnFamilyClient(cm.getHost(), server.getConfiguration().getTransportPort());
        mapping.putIfAbsent(destination, cc);
        return cc;
      }
    }
    throw new RuntimeException(String.format(
            "destination %s does not exist. Live members %s. Dead members %s", destination.getDestinationId(), 
            server.getClusterMembership().getLiveMembers(), server.getClusterMembership().getDeadMembers()));
  }
  
  
  @Override
  public void run() {
    while (goOn){
      List<ClusterMember> live = server.getClusterMembership().getLiveMembers();
      for (ClusterMember member : live){
        SortedMap<AtomKey,AtomValue> results = hintCf.slice(member.getId(), "", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
        if (results.size() > 0){
          for (Entry<AtomKey, AtomValue> i : results.entrySet()){
            if (i.getValue() instanceof ColumnValue){
              ColumnValue v = (ColumnValue) i.getValue();
              ColumnKey c = (ColumnKey) i.getKey();
              writeAndDelete(member, c, v);
            }
          }
        }
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) { }
    }
  }

  
  private void writeAndDelete(ClusterMember m, ColumnKey c, ColumnValue v){
    Destination d = new Destination();
    d.setDestinationId(m.getId());
    Client client = this.clientForDestination(d);
    try {
      client.post( om.readValue(v.getValue(), Message.class));
      hintCf.delete(m.getId(), c.getColumn(), System.currentTimeMillis() * 1000);
      hintsDelivered.getAndIncrement();
    } catch ( IOException | RuntimeException e) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e1) {
      }
      hintsFailed.getAndIncrement();
    }
  }

  public long getHintsDelivered() {
    return hintsDelivered.get();
  }

  public long getHintsFailed() {
    return hintsFailed.get();
  }
  
}
