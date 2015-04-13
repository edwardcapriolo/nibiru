package io.teknek.nibiru.personality;

public interface ColumnFamilyAdminPersonality {

  static final String PERSONALITY = "ADMIN_PERSONALITY";
  static final String CLEANUP = "CLEANUP";
  
  public void cleanup(String keyspace, String columnFamily);
}
