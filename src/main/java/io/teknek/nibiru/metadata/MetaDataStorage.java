package io.teknek.nibiru.metadata;

import java.util.Map;
import io.teknek.nibiru.Configuration;

public interface MetaDataStorage {
  void persist(Configuration configuration, Map<String,KeyspaceAndColumnFamilyMetaData> writeThis);
  Map<String,KeyspaceAndColumnFamilyMetaData> read(Configuration configuration);
}
