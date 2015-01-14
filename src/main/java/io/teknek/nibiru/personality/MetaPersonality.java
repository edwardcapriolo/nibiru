package io.teknek.nibiru.personality;

import java.util.Map;

public interface MetaPersonality {

  public static final String CREATE_OR_UPDATE_KEYSPACE = "CREATE_OR_UPDATE_KEYSPACE";
  public static final String META_PERSONALITY = "META_PERSONALITY";
  
  void createOrUpdateKeyspace(String keyspace, Map<String,Object> properties);
}
