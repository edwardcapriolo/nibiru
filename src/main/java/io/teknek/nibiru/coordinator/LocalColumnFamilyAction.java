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

import io.teknek.nibiru.ColumnFamily;
import io.teknek.nibiru.Keyspace;
import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.AtomKey;
import io.teknek.nibiru.engine.ColumnKey;
import io.teknek.nibiru.personality.ColumnFamilyPersonality;
import io.teknek.nibiru.transport.Message;
import io.teknek.nibiru.transport.Response;

public class LocalColumnFamilyAction extends LocalAction {
  

  public LocalColumnFamilyAction(Message message, Keyspace ks, ColumnFamily cf){
    super(message,ks,cf);
  }
  
  @Override
  public Response handleReqest() {
    if (! (this.columnFamily instanceof ColumnFamilyPersonality)){
      throw new RuntimeException("Column Family "+columnFamily.getColumnFamilyMetadata().getName() 
              + "does not support " + ColumnFamilyPersonality.COLUMN_FAMILY_PERSONALITY );
    }
    ColumnFamilyPersonality personality = (ColumnFamilyPersonality) this.columnFamily;
    if (message.getPayload().get("type").equals("get")){
      Val v = personality.get(
              (String) message.getPayload().get("rowkey"),
              (String) message.getPayload().get("column"));
      Response r = new Response();
      r.put("payload", v);
      return r;
    } else if (message.getPayload().get("type").equals("slice")){
      SortedMap<AtomKey,Val> res = personality.slice(
              (String) message.getPayload().get("rowkey"),
              (String) message.getPayload().get("start"),
              (String) message.getPayload().get("end") 
              );
      SortedMap<String,Val> res2 = new TreeMap<>();
      //TODO bug here
      for (Map.Entry<AtomKey, Val> column: res.entrySet() ){
        res2.put(((ColumnKey) column.getKey()).getColumn(), column.getValue());
      }
      return new Response().withProperty("payload", res2);
    } else if (message.getPayload().get("type").equals("put")) {
      Long l = ((Long) message.getPayload().get("ttl"));
      if (l == null){
        personality.put(
              (String) message.getPayload().get("rowkey"),
              (String) message.getPayload().get("column"),
              (String) message.getPayload().get("value"),
              ((Number) message.getPayload().get("time")).longValue());
        return new Response();
      } else {
        personality.put(
                (String) message.getPayload().get("rowkey"),
                (String) message.getPayload().get("column"),
                (String) message.getPayload().get("value"),
                ((Number) message.getPayload().get("time")).longValue(), l);
        return new Response();
      }
    } else if (message.getPayload().get("type").equals("delete")) { 
      personality.delete(
              (String) message.getPayload().get("rowkey"),
              (String) message.getPayload().get("column"),
              ((Number) message.getPayload().get("time")).longValue());
      return new Response();
    } else {
      throw new RuntimeException("Does not support this type of message");
    }    
  }

}
