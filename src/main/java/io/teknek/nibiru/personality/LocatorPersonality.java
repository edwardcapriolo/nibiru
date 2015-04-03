package io.teknek.nibiru.personality;

import io.teknek.nibiru.ContactInformation;
import io.teknek.nibiru.Destination;

import java.util.List;

public interface LocatorPersonality {
  static final String PERSONALITY = "LOCATOR_PERSONALITY";
  static final String LOCATE_ROW_KEY = "LOCATE_ROW_KEY";
  List<ContactInformation> locateRowKey(List<Destination> destinations);
  
}
