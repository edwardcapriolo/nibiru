package io.teknek.nibiru;

public class Destination {
  private String destinationId;
  
  public Destination(){
    
  }
  
  public Destination(String destinationId){
    this.destinationId  = destinationId;
  }
  
  public String getDestinationId() {
    return destinationId;
  }
  
  public void setDestinationId(String destinationId) {
    this.destinationId = destinationId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((destinationId == null) ? 0 : destinationId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Destination other = (Destination) obj;
    if (destinationId == null) {
      if (other.destinationId != null)
        return false;
    } else if (!destinationId.equals(other.destinationId))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Destination [destinationId=" + destinationId + "]";
  }
  
}