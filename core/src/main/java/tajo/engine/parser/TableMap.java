/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.parser;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import tajo.catalog.exception.AlreadyExistsTableException;

import java.util.Map;
import java.util.Set;

public class TableMap {
  private Map<String, String> aliasToNameMap = Maps.newHashMap();
  private Map<String, Set<String>> nameToAliasMap = Maps.newHashMap();

  public boolean exists(String alias) {
    return aliasToNameMap.containsKey(alias);
  }

  public void addFromTable(String tableName, String alias) {
    if (aliasToNameMap.containsKey(alias)) {
      throw new AlreadyExistsTableException();
    }
    aliasToNameMap.put(alias, tableName);

    if (nameToAliasMap.containsKey(tableName)) {
      Preconditions.checkState(!nameToAliasMap.get(tableName).contains(alias),
          "There is inconsistency of the map between name and alias");
      nameToAliasMap.get(tableName).add(alias);
    } else {
      nameToAliasMap.put(tableName, Sets.newHashSet(alias));
    }
  }

  public void addFromTable(QueryBlock.FromTable fromTable) {
    addFromTable(fromTable.getTableName(), fromTable.getTableId());
  }

  public String getTableNameByAlias(String alias) {
    return aliasToNameMap.get(alias);
  }

  public Iterable<String> getAllTableNames() {
    return nameToAliasMap.keySet();
  }
}
