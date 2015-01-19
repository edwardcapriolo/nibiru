/*
 * Copyright 2015 Edward Capriolo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
