package io.teknek.nibiru;

import java.util.HashMap;
import java.util.Map;

public class Consistency {
  private ConsistencyLevel level;
  private Map<String,Object> parameters;
  
  public Consistency(){
    parameters = new HashMap<>();
  }
  
  public Consistency(ConsistencyLevel level, Map<String,Object> parameters){
    this.parameters = parameters;
    this.level = level;
  }

  public ConsistencyLevel getLevel() {
    return level;
  }

  public void setLevel(ConsistencyLevel level) {
    this.level = level;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> params) {
    this.parameters = params;
  }
  
  public Consistency withParameter(String name, Object object){
    this.parameters.put(name, object);
    return this;
  }
  
  public Consistency withLevel(ConsistencyLevel level){
    this.level = level;
    return this;
  }

  public Consistency withParameters(Map<String, Object> parameters) {
    setParameters(parameters);
    return this;
  }
}
