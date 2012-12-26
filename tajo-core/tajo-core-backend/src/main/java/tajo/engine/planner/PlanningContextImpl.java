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

package tajo.engine.planner;

import org.antlr.runtime.tree.Tree;
import tajo.engine.parser.ParseTree;

/**
 * @author Hyunsik Choi
 */
public class PlanningContextImpl implements PlanningContext  {
  private String rawQuery;
  // private TableMap tableMap = new TableMap();
  private ParseTree parseTree;
  private Tree ast;
  private String outputTableName;

  private static final String DEFAULT_TABLE_GEN_PREFIX = "table";
  private static final String DEFAULT_COLUMN_GEN_PREFIX = "column";
  private static final String SEPARATOR = "_";

  private int generatedTableId = 1;
  private int generatedColumnId = 1;


  public PlanningContextImpl(String rawQuery) {
    this.rawQuery = rawQuery;
  }

  @Override
  public String getRawQuery() {
    return rawQuery;
  }

  @Override
  public Tree getAST() {
    return ast;
  }

  public void setAST(Tree ast) {
    this.ast = ast;
  }

  @Override
  public ParseTree getParseTree() {
    return parseTree;
  }

//  @Override
//  public TableMap getTableMap() {
//    return tableMap;
//  }

  @Override
  public String getGeneratedColumnName() {
    return DEFAULT_COLUMN_GEN_PREFIX + SEPARATOR + (generatedColumnId++);
  }

  @Override
  public synchronized String getGeneratedColumnName(String prefix) {
    return prefix + SEPARATOR + (generatedColumnId++);
  }

  @Override
  public String getExplicitOutputTable() {
    return outputTableName;
  }

  @Override
  public boolean hasExplicitOutputTable() {
    return outputTableName != null;
  }

  public void setOutputTableName(String outputTableName) {
    this.outputTableName = outputTableName;
  }

  public void setParseTree(ParseTree parseTree) {
    this.parseTree = parseTree;
  }
}
