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

import java.util.Map;

public interface MetaPersonality {

  public static final String CREATE_OR_UPDATE_KEYSPACE = "CREATE_OR_UPDATE_KEYSPACE";
  public static final String CREATE_OR_UPDATE_STORE = "CREATE_OR_UPDATE_STORE";
  public static final String META_PERSONALITY = "META_PERSONALITY";
  public static final String LIST_LIVE_MEMBERS = "LIST_LIVE_MEMBERS";
  public static final String LIST_DEAD_MEMBERS = "LIST_DEAD_MEMBERS";
  public static final String LIST_KEYSPACES = "LIST_KEYSPACES";
  public static final String LIST_STORES = "LIST_STORES";
  public static final String GET_KEYSPACE_METADATA = "GET_KEYPSACE_METADATA";
  
  void createOrUpdateKeyspace(String keyspace, Map<String,Object> properties);
}
