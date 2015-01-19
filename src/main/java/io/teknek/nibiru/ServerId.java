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

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.UUID;

public class ServerId {

  private final Configuration configuration;
  private UUID u;
  
  public ServerId(Configuration configuration){
    this.configuration = configuration;
  }
  
  public void init(){
    File uuid = new File(configuration.getDataDirectory(), "server.id");
    if (!uuid.exists()){
      u = UUID.randomUUID();
      XMLEncoder e ;
      try {
        e = new XMLEncoder (new BufferedOutputStream(new FileOutputStream(uuid)));
      } catch (FileNotFoundException ex){
        throw new RuntimeException("im dead", ex);
      }
      e.writeObject(u.toString());
      e.close();
    } else {
      XMLDecoder d;
      try {
        d = new XMLDecoder(
                new BufferedInputStream(
                    new FileInputStream(uuid)));
        String suuid = (String) d.readObject();
        u = UUID.fromString(suuid);
      } catch (FileNotFoundException e) {
        throw new RuntimeException("im dead", e);
      }
      d.close();
    }
  }

  public UUID getU() {
    return u;
  }
  
}
