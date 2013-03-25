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

import tajo.engine.planner.PlanningContext;

import java.util.Map.Entry;

public class SetStmt extends ParseTree {
  private ParseTree leftTree;
  private ParseTree rightTree;
  private boolean distinct = true;

  public SetStmt(final PlanningContext context,
                 final StatementType type,
                 final ParseTree leftTree,
                 final ParseTree rightTree,
                 boolean distinct) {
    super(context, type);
    this.leftTree = leftTree;
    this.rightTree = rightTree;
    this.distinct = distinct;

    for (Entry<String, String> entry : leftTree.getAliasToNames()) {
       addTableRef(entry.getValue(), entry.getKey());
    }

    for (Entry<String, String> entry : rightTree.getAliasToNames()) {
      addTableRef(entry.getValue(), entry.getKey());
    }
  }
  
  public boolean isDistinct() {
    return distinct;
  }
  
  public ParseTree getLeftTree() {
    return this.leftTree;
  }
  
  public ParseTree getRightTree() {
    return this.rightTree;
  }
}
