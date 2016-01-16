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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import io.teknek.nibiru.Store;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;
import io.teknek.nibiru.engine.atom.ColumnKey;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.BaseMessage;
import io.teknek.nibiru.transport.Response;
import io.teknek.nibiru.transport.columnfamily.DeleteMessage;
import io.teknek.nibiru.transport.columnfamily.GetMessage;
import io.teknek.nibiru.transport.columnfamily.PutMessage;
import io.teknek.nibiru.transport.columnfamily.SliceMessage;

public class LocalColumnFamilyAction extends LocalAction {
  
  public LocalColumnFamilyAction(BaseMessage message, Keyspace ks, Store cf){
    super(message,ks,cf);
  }
  
  @Override
  public Response handleReqest() {
    if (! (this.columnFamily instanceof ColumnFamilyPersonality)){
      throw new RuntimeException("Column Family " + columnFamily.getStoreMetadata().getName() 
              + "does not support Column Family operations"   );
    }
    ColumnFamilyPersonality personality = (ColumnFamilyPersonality) this.columnFamily;
    if (message instanceof GetMessage){
      GetMessage g = (GetMessage) message;
      AtomValue v = personality.get(g.getRow(), g.getColumn());
      return new Response().withProperty("payload", v);
    } else if (message instanceof PutMessage){
      PutMessage m = (PutMessage) message;
      Number l = m.getTtl();
      if (l == null){
        personality.put(m.getRow(), m.getColumn(), m.getValue(), m.getVersion());
        return new Response();
      } else {
        personality.put(m.getRow(), m.getColumn(), m.getValue(), m.getVersion(), l.longValue());
        return new Response();
      }
    } else if (message instanceof DeleteMessage){
      DeleteMessage d = (DeleteMessage) message;
      personality.delete(d.getRow(), d.getColumn(), d.getVersion());
      return new Response();
    } else if (message instanceof SliceMessage) {
      SliceMessage m = (SliceMessage ) message;
      SortedMap<AtomKey,AtomValue> res = personality.slice(m.getRow(), m.getStart(), m.getEnd() );
      SortedMap<String,AtomValue> res2 = new TreeMap<>();
      //TODO bug here
      for (Map.Entry<AtomKey, AtomValue> column: res.entrySet() ){
        res2.put(((ColumnKey) column.getKey()).getColumn(), column.getValue());
      }
      return new Response().withProperty("payload", res2);
    } else {
      throw new RuntimeException("Does not support this type of message");
    }    
  }

}
