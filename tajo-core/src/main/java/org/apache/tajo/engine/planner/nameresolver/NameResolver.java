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

package org.apache.tajo.engine.planner.nameresolver;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.exception.AmbiguousFieldException;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.exception.VerifyException;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * NameResolver utility
 */
public abstract class NameResolver {

  public static Map<NameResolvingMode, NameResolver> resolverMap = Maps.newHashMap();

  static {
    resolverMap.put(NameResolvingMode.RELS_ONLY, new ResolverByRels());
    resolverMap.put(NameResolvingMode.RELS_AND_SUBEXPRS, new ResolverByRelsAndSubExprs());
    resolverMap.put(NameResolvingMode.SUBEXPRS_AND_RELS, new ResolverBySubExprsAndRels());
    resolverMap.put(NameResolvingMode.LEGACY, new ResolverByLegacy());
  }

  abstract Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
  throws PlanningException;

  /**
   * Try to find the database name
   *
   * @param block the current block
   * @param tableName The table name
   * @return The found database name
   * @throws PlanningException
   */
  public static String resolveDatabase(LogicalPlan.QueryBlock block, String tableName) throws PlanningException {
    List<String> found = new ArrayList<String>();
    for (RelationNode relation : block.getRelations()) {
      // check alias name or table name
      if (CatalogUtil.extractSimpleName(relation.getCanonicalName()).equals(tableName) ||
          CatalogUtil.extractSimpleName(relation.getTableName()).equals(tableName)) {
        // obtain the database name
        found.add(CatalogUtil.extractQualifier(relation.getTableName()));
      }
    }

    if (found.size() == 0) {
      return null;
    } else if (found.size() > 1) {
      throw new PlanningException("Ambiguous table name \"" + tableName + "\"");
    }

    return found.get(0);
  }

  public static Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr column,
                        NameResolvingMode mode) throws PlanningException {
    if (!resolverMap.containsKey(mode)) {
      throw new PlanningException("Unsupported name resolving level: " + mode.name());
    }
    return resolverMap.get(mode).resolve(plan, block, column);
  }

  /**
   * Try to find a column from all relations within a given query block.
   * If a given column reference is qualified, it tries to resolve the name
   * from only the relation corresponding to the qualifier.
   *
   * @param plan The logical plan
   * @param block The current query block
   * @param columnRef The column reference to be found
   * @return The found column
   * @throws PlanningException
   */
  static Column resolveFromRelsWithinBlock(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                  ColumnReferenceExpr columnRef) throws PlanningException {
    String qualifier;
    String canonicalName;

    if (columnRef.hasQualifier()) {
      Pair<String, String> normalized = normalizeQualifierAndCanonicalName(block, columnRef);
      qualifier = normalized.getFirst();
      canonicalName = normalized.getSecond();

      RelationNode relationOp = block.getRelation(qualifier);

      // If we cannot find any relation against a qualified column name
      if (relationOp == null) {
        throw null;
      }

      // Please consider a query case:
      // select lineitem.l_orderkey from lineitem a order by lineitem.l_orderkey;
      //
      // The relation lineitem is already renamed to "a", but lineitem.l_orderkey still can be used.
      // The below code makes it available. Otherwise, it cannot find any match in the relation schema.
      if (block.isAlreadyRenamedTableName(CatalogUtil.extractQualifier(canonicalName))) {
        canonicalName =
            CatalogUtil.buildFQName(relationOp.getCanonicalName(), CatalogUtil.extractSimpleName(canonicalName));
      }

      Schema schema = relationOp.getTableSchema();
      Column column = schema.getColumn(canonicalName);

      return column;
    } else {
      return resolveFromAllRelsInBlock(block, columnRef);
    }
  }

  /**
   * Try to find the column from the current node and child node. It can find subexprs generated from the optimizer.
   *
   * @param block The current query block
   * @param columnRef The column reference to be found
   * @return The found column
   */
  static Column resolveFromCurrentAndChildNode(LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
      throws NoSuchColumnException {

    if (block.getCurrentNode() != null && block.getCurrentNode().getInSchema() != null) {
      Column found = block.getCurrentNode().getInSchema().getColumn(columnRef.getCanonicalName());
      if (found != null) {
        return found;
      } else if (block.getLatestNode() != null) {
        found = block.getLatestNode().getOutSchema().getColumn(columnRef.getName());
        if (found != null) {
          return found;
        }
      }
    }
    return null;
  }

  /**
   * It tries to find a full qualified column name from all relations in the current block.
   *
   * @param block The current query block
   * @param columnRef The column reference to be found
   * @return The found column
   */
  static Column resolveFromAllRelsInBlock(LogicalPlan.QueryBlock block,
                                          ColumnReferenceExpr columnRef) throws VerifyException {
    List<Column> candidates = TUtil.newList();

    for (RelationNode rel : block.getRelations()) {
      Column found = rel.getTableSchema().getColumn(columnRef.getName());
      if (found != null) {
        candidates.add(found);
      }
    }

    if (!candidates.isEmpty()) {
      return ensureUniqueColumn(candidates);
    } else {
      return null;
    }
  }

  /**
   * Trying to find a column from all relations in other blocks
   *
   * @param plan The logical plan
   * @param columnRef The column reference to be found
   * @return The found column
   */
  static Column resolveFromAllRelsInAllBlocks(LogicalPlan plan, ColumnReferenceExpr columnRef) throws VerifyException {

    List<Column> candidates = Lists.newArrayList();

    // from all relations of all query blocks
    for (LogicalPlan.QueryBlock eachBlock : plan.getQueryBlocks()) {

      for (RelationNode rel : eachBlock.getRelations()) {
        Column found = rel.getTableSchema().getColumn(columnRef.getName());
        if (found != null) {
          candidates.add(found);
        }
      }
    }

    if (!candidates.isEmpty()) {
      return NameResolver.ensureUniqueColumn(candidates);
    } else {
      return null;
    }
  }

  /**
   * Try to find a column from the final schema of the current block.
   *
   * @param block The current query block
   * @param columnRef The column reference to be found
   * @return The found column
   */
  static Column resolveAliasedName(LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef) throws VerifyException {
    List<Column> candidates = Lists.newArrayList();

    if (block.getSchema() != null) {
      Column found = block.getSchema().getColumn(columnRef.getName());
      if (found != null) {
        candidates.add(found);
      }
    }

    if (!candidates.isEmpty()) {
      return NameResolver.ensureUniqueColumn(candidates);
    } else {
      return null;
    }
  }

  /**
   * It returns a pair of names, which the first value is ${database}.${table} and the second value
   * is a simple column name.
   *
   * @param block The current block
   * @param columnRef The column name
   * @return A pair of normalized qualifier and column name
   * @throws PlanningException
   */
  static Pair<String, String> normalizeQualifierAndCanonicalName(LogicalPlan.QueryBlock block,
                                                                 ColumnReferenceExpr columnRef)
      throws PlanningException {
    String qualifier;
    String canonicalName;

    if (CatalogUtil.isFQTableName(columnRef.getQualifier())) {
      qualifier = columnRef.getQualifier();
      canonicalName = columnRef.getCanonicalName();
    } else {
      String resolvedDatabaseName = resolveDatabase(block, columnRef.getQualifier());
      if (resolvedDatabaseName == null) {
        throw new NoSuchColumnException(columnRef.getQualifier());
      }
      qualifier = CatalogUtil.buildFQName(resolvedDatabaseName, columnRef.getQualifier());
      canonicalName = CatalogUtil.buildFQName(qualifier, columnRef.getName());
    }

    return new Pair<String, String>(qualifier, canonicalName);
  }

  static Column ensureUniqueColumn(List<Column> candidates) throws VerifyException {
    if (candidates.size() == 1) {
      return candidates.get(0);
    } else if (candidates.size() > 2) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      for (Column column : candidates) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(column);
      }
      throw new AmbiguousFieldException("Ambiguous Column Name: " + sb.toString());
    } else {
      return null;
    }
  }
}
