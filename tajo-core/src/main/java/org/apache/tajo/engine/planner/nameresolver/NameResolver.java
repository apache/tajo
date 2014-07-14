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

public abstract class NameResolver {

  public static Map<NameResolveLevel, NameResolver> resolverMap = Maps.newHashMap();

  static {
    resolverMap.put(NameResolveLevel.RELS_ONLY, new ResolverByRels());
    resolverMap.put(NameResolveLevel.RELS_AND_SUBEXPRS, new ResolverByRelsAndSubExprs());
    resolverMap.put(NameResolveLevel.SUBEXPRS_AND_RELS, new ResolverBySubExprsAndRels());
    resolverMap.put(NameResolveLevel.GLOBAL, new ResolverByLegacy());
  }

  public static Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr column,
                        NameResolveLevel level) throws PlanningException {
    if (!resolverMap.containsKey(level)) {
      throw new PlanningException("Unsupported name resolving level");
    }
    return resolverMap.get(level).resolve(plan, block, column);
  }

  public static Column ensureUniqueColumn(List<Column> candidates)
      throws VerifyException {
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

  /**
   * It tries to resolve the variable name from relations within a given query block.
   * If a variable name is qualified, it tries to resolve the name from the relation corresponding to the qualifier.
   *
   * @param plan
   * @param block
   * @param columnRef
   * @return
   * @throws PlanningException
   */
  public static Column resolveFromRelsWithinBlock(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                  ColumnReferenceExpr columnRef) throws PlanningException {
    String qualifier;
    String canonicalName;

    if (columnRef.hasQualifier()) {
      Pair<String, String> normalized = normalizeQualifierAndCanonicalName(plan, block, columnRef);
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
      return resolveUnqualifiedFromAllRelsInBlock(plan, block, columnRef);
    }
  }

  public static Column resolveSubExprReferences(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                ColumnReferenceExpr columnRef) throws NoSuchColumnException {
    // Trying to find the column within the current block
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

  public static Column resolveUnqualifiedFromAllRelsInBlock(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                            ColumnReferenceExpr column) throws VerifyException {
    List<Column> candidates = TUtil.newList();

    // It tries to find a full qualified column name from all relations in the current block.
    for (RelationNode rel : block.getRelations()) {
      Column found = rel.getTableSchema().getColumn(column.getName());
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

  public static Column resolveFromAllRelsInAllBlocks(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                     ColumnReferenceExpr columnRef) throws VerifyException {
    List<Column> candidates = Lists.newArrayList();
    // Trying to find columns from other relations in other blocks
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

  public static Column resolveAliasedName(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                          ColumnReferenceExpr columnRef) throws VerifyException {
    List<Column> candidates = Lists.newArrayList();
    // Trying to find columns from schema in current block.
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

  public static Pair<String, String> normalizeQualifierAndCanonicalName(LogicalPlan plan, LogicalPlan.QueryBlock block,
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

  abstract Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
      throws PlanningException;
}
