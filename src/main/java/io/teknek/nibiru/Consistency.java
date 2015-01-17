package io.teknek.nibiru;

import java.util.HashMap;
import java.util.Map;

public class Consistency {
  private ConsistencyLevel level;
  private Map<String,Object> params;
  
  public Consistency(){
    params = new HashMap<>();
  }
  
  public Consistency(ConsistencyLevel level, Map<String,Object> params){
    this.params = params;
    this.level = level;
  }

  public ConsistencyLevel getLevel() {
    return level;
  }

  public void setLevel(ConsistencyLevel level) {
    this.level = level;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params;
  }
  
  public Consistency withParamater(String name, Object object){
    this.params.put(name, object);
    return this;
  }
  
  public Consistency withLevel(ConsistencyLevel level){
    this.level = level;
    return this;
  }

  public Consistency withParameters(Map<String, Object> parameters) {
    setParams(parameters);
    return this;
  }
}
