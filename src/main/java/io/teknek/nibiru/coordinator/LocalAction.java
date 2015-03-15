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
package io.teknek.nibiru.coordinator;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

//potential callable
public abstract class LocalAction {

  protected Message message;
  protected Keyspace keyspace; 
  protected Store columnFamily;
  
  public LocalAction(Message message, Keyspace ks, Store cf){
    this.message = message;
    this.keyspace = ks;
    this.columnFamily = cf;
  }
  
  public abstract Response handleReqest();
}
