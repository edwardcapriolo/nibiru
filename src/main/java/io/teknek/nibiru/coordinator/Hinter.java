package io.teknek.nibiru.coordinator;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import io.teknek.nibiru.Destination;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;

public class Hinter {

  private static final AtomicLong HINTS = new AtomicLong();
  private final ColumnFamilyPersonality hintsColumnFamily;
  private ObjectMapper OM = new ObjectMapper();
  
  public Hinter(ColumnFamilyPersonality person){
    this.hintsColumnFamily = person;
  }
  
  public void hint(Message m, Destination destination) {
    try {
      hintsColumnFamily.put(destination.getDestinationId(), UUID.randomUUID().toString(),
              OM.writeValueAsString(m), System.currentTimeMillis() * 1000L);
      HINTS.getAndIncrement();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
