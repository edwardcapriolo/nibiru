package io.teknek.nibiru;

public class TriggerDefinition {
  private TriggerLevel triggerLevel;
  private String triggerClass;
  
  public TriggerDefinition(){
    
  }
  
  public TriggerLevel getTriggerLevel() {
    return triggerLevel;
  }
  
  public void setTriggerLevel(TriggerLevel triggerLevel) {
    this.triggerLevel = triggerLevel;
  }
  
  public String getTriggerClass() {
    return triggerClass;
  }
  
  public void setTriggerClass(String triggerClass) {
    this.triggerClass = triggerClass;
  }
  
}
