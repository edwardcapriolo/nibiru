package io.teknek.nibiru.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import io.teknek.nibiru.Configuration;

public interface MetaDataStorage {
  void persist(Configuration configuration, Map<String,KeyspaceMetadata> writeThis);
  Map<String,KeyspaceMetadata> read(Configuration configuration);
}
