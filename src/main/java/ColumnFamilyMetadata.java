
public class ColumnFamilyMetadata {
  private String name;
  private long tombstoneGraceTime;
  
  public ColumnFamilyMetadata(){
    
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getTombstoneGraceTime() {
    return tombstoneGraceTime;
  }

  public void setTombstoneGraceTime(long tombstoneGraceTime) {
    this.tombstoneGraceTime = tombstoneGraceTime;
  }
  
}
