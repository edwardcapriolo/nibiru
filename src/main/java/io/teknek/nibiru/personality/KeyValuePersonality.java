package io.teknek.nibiru.personality;

public interface KeyValuePersonality {
  public static final String KEY_VALUE_PERSONALITY = "KEY_VALUE_PERSONALITY";
  public void put(String key, String value);
  public String get(String key);
}
