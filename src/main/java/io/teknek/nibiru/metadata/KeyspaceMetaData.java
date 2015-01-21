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
package io.teknek.nibiru.metadata;

import io.teknek.nibiru.partitioner.NaturalPartitioner;
import io.teknek.nibiru.partitioner.Partitioner;
import io.teknek.nibiru.router.LocalRouter;
import io.teknek.nibiru.router.Router;

import java.util.Map;


public class KeyspaceMetaData {
  public static final String PARTITIONER_CLASS = "partitioner_class";
  public static final String ROUTER_CLASS = "router_class";
  private String name;
  private Map<String, Object> properties;
  private transient Partitioner partitioner;
  private transient Router router;
  
  //serialization
  public KeyspaceMetaData(){
    
  }
  
  public KeyspaceMetaData(String name, Map<String,Object> properties){
    this.name = name;
    setProperties(properties);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Partitioner getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
  }

  public Router getRouter() {
    return router;
  }

  public void setRouter(Router router) {
    this.router = router;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
    if (properties.containsKey(PARTITIONER_CLASS)){
      try {
        partitioner = (Partitioner) Class.forName((String) properties.get(PARTITIONER_CLASS)).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      partitioner = new NaturalPartitioner();
    }
    if (properties.containsKey(ROUTER_CLASS)){
      try {
        router = (Router) Class.forName((String) properties.get(ROUTER_CLASS)).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    } else {
      router = new LocalRouter();
    }
  }

  @Override
  public String toString() {
    return "KeyspaceMetaData [name=" + name + ", properties=" + properties + "]";
  }

}
