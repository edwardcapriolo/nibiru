package io.teknek.nibiru.coordinator;

import java.util.SortedMap;
import java.util.TreeMap;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.engine.DirectSsTableWriter;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomPair;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.directsstable.Close;
import io.teknek.nibiru.transport.directsstable.DirectSsTableMessage;
import io.teknek.nibiru.transport.directsstable.Open;
import io.teknek.nibiru.transport.directsstable.Write;

public class DirectSsTableCoordinator {

  private final Server server;
  
  public DirectSsTableCoordinator(Server server){
    this.server = server;
  }
  
  public Response handleStreamRequest(DirectSsTableMessage m){
    Store store = server.getKeyspaces().get(m.getKeyspace())
            .getStores().get(m.getStore());
    DirectSsTableWriter w = (DirectSsTableWriter) store;
    if (m instanceof Open){
      w.open(m.getId());
      return new Response();
    } else if (m instanceof Close){
      w.close(m.getId());
    } else if (m instanceof Write){
      Write write = (Write) m;
      SortedMap<AtomKey, AtomValue> mp = new TreeMap<>();
      for (AtomPair aPair: write.getColumns()){
        mp.put(aPair.getKey(), aPair.getValue());
      }
      w.write(write.getToken(), mp, write.getId());
    } else {
      throw new RuntimeException("hit rock bottom");
    }
    return null;
  }
  
}
