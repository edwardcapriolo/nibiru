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
package io.teknek.nibiru.personality;

import java.util.SortedMap;

import io.teknek.nibiru.Val;
import io.teknek.nibiru.engine.atom.AtomKey;
import io.teknek.nibiru.engine.atom.AtomValue;

public interface ColumnFamilyPersonality {

  public static final String COLUMN_FAMILY_PERSONALITY = "COLUMN_FAMILY_PERSONALITY";
          
  public abstract AtomValue get(String rowkey, String column);

  public abstract void delete(String rowkey, String column, long time);

  public abstract void put(String rowkey, String column, String value, long time, long ttl);

  public abstract void put(String rowkey, String column, String value, long time);
  
  public abstract SortedMap<AtomKey, AtomValue> slice(String rowkey, String start, String end);
}
