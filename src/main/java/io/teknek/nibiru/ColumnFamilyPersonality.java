package io.teknek.nibiru;

public interface ColumnFamilyPersonality {

  public abstract Val get(String rowkey, String column);

  public abstract void delete(String rowkey, String column, long time);

  public abstract void put(String rowkey, String column, String value, long time, long ttl);

  public abstract void put(String rowkey, String column, String value, long time);
}
