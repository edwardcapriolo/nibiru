package io.teknek.nibiru;

public class ContactInformation {
  private Destination destination;
  private String transportHost;
  
  public ContactInformation(){
    
  }

  public ContactInformation(Destination destination, String transportHost){
    this.destination = destination;
    this.transportHost = transportHost;
  }
  
  public Destination getDestination() {
    return destination;
  }

  public void setDestination(Destination destination) {
    this.destination = destination;
  }

  public String getTransportHost() {
    return transportHost;
  }

  public void setTransportHost(String transportHost) {
    this.transportHost = transportHost;
  }

}
