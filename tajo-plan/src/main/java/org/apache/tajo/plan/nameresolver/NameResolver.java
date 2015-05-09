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

package org.apache.tajo.plan.nameresolver;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.algebra.Relation;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.NestedPathUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.exception.NoSuchColumnException;
import org.apache.tajo.plan.algebra.AmbiguousFieldException;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.plan.logical.RelationNode;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Column name resolution utility. A SQL statement can include many kinds of column names,
 * defined in different ways. Some column name indicates just a column in a relation.
 * Another column name includes alias table name or alias column name, derived from some expression.
 *
 * This utility ensures that each column name is derived from valid and accessible column, and
 * it also finds the exact data type of the column.
 *
 * Terminology:
 * <ul>
 *   <li>Qualifier:  database name, table name, or both included in a column name</li>
 *   <li>Simple name: just column name without any qualifier</li>
 *   <li>Alias name: another name to shortly specify a certain column</li>
 *   <li>Fully qualified name: a column name with database name and table name</li>
 *   <li>Canonical name: a fully qualified name, but its simple name is aliased name.</li>
 * </ul>
 */
public abstract class NameResolver {

  public static final Map<NameResolvingMode, NameResolver> resolverMap = Maps.newHashMap();

  static {
    resolverMap.put(NameResolvingMode.RELS_ONLY, new ResolverByRels());
    resolverMap.put(NameResolvingMode.RELS_AND_SUBEXPRS, new ResolverByRelsAndSubExprs());
    resolverMap.put(NameResolvingMode.SUBEXPRS_AND_RELS, new ResolverBySubExprsAndRels());
    resolverMap.put(NameResolvingMode.LEGACY, new ResolverByLegacy());
  }

  public static Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr column,
                               NameResolvingMode mode) throws PlanningException {
    if (!resolverMap.containsKey(mode)) {
      throw new PlanningException("Unsupported name resolving level: " + mode.name());
    }
    return resolverMap.get(mode).resolve(plan, block, column);
  }

  abstract Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
  throws PlanningException;

  /**
   * Guess a relation from a table name regardless of whether the given name is qualified or not.
   *
   * @param block the current block
   * @param tableName The table name which can be either qualified or not.
   * @return A corresponding relation
   * @throws PlanningException
   */
  public static RelationNode lookupTable(LogicalPlan.QueryBlock block, String tableName) throws PlanningException {
    List<RelationNode> found = TUtil.newList();

    for (RelationNode relation : block.getRelations()) {

      // if a table name is qualified
      if (relation.getCanonicalName().equals(tableName) || relation.getTableName().equals(tableName)) {
        found.add(relation);

      // if a table name is not qualified
      } else if (CatalogUtil.extractSimpleName(relation.getCanonicalName()).equals(tableName) ||
          CatalogUtil.extractSimpleName(relation.getTableName()).equals(tableName)) {
        found.add(relation);
      }
    }

    if (found.size() == 0) {
      return null;

    } else if (found.size() > 1) {
      throw new PlanningException("Ambiguous table name \"" + tableName + "\"");
    }

    return found.get(0);
  }

  /**
   * Find relations such that its schema contains a given column
   *
   * @param block the current block
   * @param columnName The column name to find relation
   * @return relations including a given column
   * @throws PlanningException
   */
  public static Collection<RelationNode> lookupTableByColumns(LogicalPlan.QueryBlock block, String columnName)
      throws PlanningException {

    Set<RelationNode> found = TUtil.newHashSet();

    for (RelationNode rel : block.getRelations()) {
      if (rel.getLogicalSchema().contains(columnName)) {
        found.add(rel);
      }
    }

    return found;
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
      Pair<String, String> normalized = lookupQualifierAndCanonicalName(block, columnRef);
      qualifier = normalized.getFirst();
      canonicalName = normalized.getSecond();

      RelationNode relationOp = block.getRelation(qualifier);

      // If we cannot find any relation against a qualified column name
      if (relationOp == null) {
        throw new PlanningException("Cannot find any relation for " + qualifier);
      }

      // Please consider a query case:
      // select lineitem.l_orderkey from lineitem a order by lineitem.l_orderkey;
      //
      // The relation lineitem is already renamed to "a", but lineitem.l_orderkey still should be available.
      // The below code makes it possible. Otherwise, it cannot find any match in the relation schema.
      if (block.isAlreadyRenamedTableName(CatalogUtil.extractQualifier(canonicalName))) {
        canonicalName =
            CatalogUtil.buildFQName(relationOp.getCanonicalName(), CatalogUtil.extractSimpleName(canonicalName));
      }

      Schema schema = relationOp.getLogicalSchema();
      Column column = schema.getColumn(canonicalName);

      return column;
    } else {
      return lookupColumnFromAllRelsInBlock(block, columnRef.getName());
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
   * Lookup a column among all relations in the current block from a column name.
   *
   * It assumes that <code>columnName</code> is not any qualified name.
   *
   * @param block The current query block
   * @param columnName The column reference to be found
   * @return The found column
   */
  static Column lookupColumnFromAllRelsInBlock(LogicalPlan.QueryBlock block,
                                               String columnName) throws VerifyException {
    Preconditions.checkArgument(CatalogUtil.isSimpleIdentifier(columnName));

    List<Column> candidates = TUtil.newList();

    for (RelationNode rel : block.getRelations()) {
      Column found = rel.getLogicalSchema().getColumn(columnName);
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
        Column found = rel.getLogicalSchema().getColumn(columnRef.getName());
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
   * Lookup a qualifier and a canonical name of column.
   *
   * It returns a pair of names, which the first value is the qualifier ${database}.${table} and
   * the second value is column's simple name.
   *
   * @param block The current block
   * @param columnRef The column name
   * @return A pair of normalized qualifier and column name
   * @throws PlanningException
   */
  static Pair<String, String> lookupQualifierAndCanonicalName(LogicalPlan.QueryBlock block,
                                                              ColumnReferenceExpr columnRef)
      throws PlanningException {
    Preconditions.checkArgument(columnRef.hasQualifier(), "ColumnReferenceExpr must be qualified.");

    String [] qualifierParts = columnRef.getQualifier().split("\\.");

    // This method assumes that column name consists of two or more dot chained names.
    // In this case, there must be three cases as follows:
    //
    // - dbname.tbname.column_name.nested_field...
    // - tbname.column_name.nested_field...
    // - column.nested_fieldX...

    Set<RelationNode> guessedRelations = TUtil.newHashSet();

    // this position indicates the index of column name in qualifierParts;
    // It must be 0 or more because a qualified column is always passed to lookupQualifierAndCanonicalName().
    int columnNamePosition = -1;

    // check for dbname.tbname.column_name.nested_field
    if (qualifierParts.length >= 2) {
      RelationNode rel = lookupTable(block, CatalogUtil.buildFQName(qualifierParts[0], qualifierParts[1]));
      if (rel != null) {
        guessedRelations.add(rel);
        columnNamePosition = 2;
      }
    }

    // check for tbname.column_name.nested_field
    if (qualifierParts.length >= 1) {
      RelationNode rel = lookupTable(block, qualifierParts[0]);
      if (rel != null) {
        guessedRelations.add(rel);
        columnNamePosition = 1;
      }
    }

    // column.nested_fieldX...
    if (guessedRelations.size() == 0 && qualifierParts.length == 1) {
      Collection<RelationNode> rels = lookupTableByColumns(block, qualifierParts[0]);

      if (rels.size() > 1) {
        throw new AmbiguousFieldException(columnRef.getCanonicalName());
      }

      if (rels.size() == 1) {
        guessedRelations.addAll(rels);
        columnNamePosition = 0;
      }
    }

    // throw exception if no column cannot be founded or two or more than columns are founded
    if (guessedRelations.size() == 0) {
      throw new NoSuchColumnException(columnRef.getQualifier());
    } else if (guessedRelations.size() > 1) {
      throw new AmbiguousFieldException(columnRef.getCanonicalName());
    }

    String qualifier = guessedRelations.iterator().next().getCanonicalName();
    String columnName = "";

    if (columnNamePosition >= qualifierParts.length) { // if there is no column in qualifierParts
      columnName = columnRef.getName();
    } else {
      // join a column name and its nested field names
      columnName = qualifierParts[columnNamePosition];

      // if qualifierParts include nested field names
      if (qualifierParts.length > columnNamePosition) {
        columnName += StringUtils.join(qualifierParts, "/", columnNamePosition + 1, qualifierParts.length);
      }

      // columnRef always has a leaf field name.
      columnName += "/" + columnRef.getName();
    }

    return new Pair<String, String>(qualifier, columnName);
  }

  static Column ensureUniqueColumn(List<Column> candidates) throws VerifyException {
    if (candidates.size() == 1) {
      return candidates.get(0);
    } else if (candidates.size() > 1) {
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
