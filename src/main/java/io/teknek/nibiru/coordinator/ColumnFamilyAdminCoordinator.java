package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Server;
import io.teknek.nibiru.Store;
import io.teknek.nibiru.engine.DefaultColumnFamily;
import io.teknek.nibiru.personality.ColumnFamilyAdminPersonality;
import io.teknek.nibiru.plugins.CompactionManager;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamilyadmin.*;

public class ColumnFamilyAdminCoordinator {

  private final Server server;
  
  public ColumnFamilyAdminCoordinator(Server server){
    this.server = server;
  }
  
  public Response handleMessage(final ColumnFamilyAdminMessage message){
    if (message instanceof CleanupMessage){
      final CleanupMessage cleanupMessage = (CleanupMessage) message;
      final CompactionManager compactionManager = (CompactionManager) server.getPlugins().get(CompactionManager.MY_NAME);
      if (compactionManager== null){
        return new Response().withProperty("exception", "could not find compaction manager");
      }
      Store store = server.getKeyspaces().get(cleanupMessage.getKeyspace()).getStores()
              .get(cleanupMessage.getColumnFamily());
      if (store instanceof DefaultColumnFamily){
        final DefaultColumnFamily cf = (DefaultColumnFamily) store;
        //TODO executor inside compaction manager
        new Thread(){
          public void run(){
            compactionManager.cleanupCompaction(server.getKeyspaces().get(cleanupMessage.getKeyspace()), cf);
          }
        }.start();
      } else {
        return new Response().withProperty("exception", "can only cleanup column family");
      }
    }
    return new Response().withProperty("exception", "could not handle" + message);
  }
}
