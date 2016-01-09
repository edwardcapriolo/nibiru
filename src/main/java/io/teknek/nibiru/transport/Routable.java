package io.teknek.nibiru.transport;

public interface Routable {
  String determineRoutingInformation();
  boolean getReRoute();
  void setReRoute(boolean reRoute);
}
