/**
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

import tajo.engine.parser.QueryBlock.FromTable;
import tajo.engine.planner.PlanningContext;

import java.util.Map.Entry;

public abstract class ParseTree {
  protected final PlanningContext context;
  protected final StatementType type;
  private final TableMap tableMap = new TableMap();

  public ParseTree(final PlanningContext context, final StatementType type) {
    this.context = context;
    this.type = type;
  }

  public StatementType getStatementType() {
    return this.type;
  }

  protected void addTableRef(FromTable table) {
    tableMap.addFromTable(table);
  }

  protected void addTableRef(String tableName, String alias) {
    tableMap.addFromTable(tableName, alias);
  }

  public StatementType getType() {
    return this.type;
  }

  public String getTableNameByAlias(String alias) {
    return tableMap.getTableNameByAlias(alias);
  }

  public Iterable<String> getAllTableNames() {
    return tableMap.getAllTableNames();
  }

  public Iterable<Entry<String, String>> getAliasToNames() {
    return tableMap.getAliasToNames();
  }
}
