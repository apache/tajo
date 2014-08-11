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

package org.apache.tajo.engine.planner.nameresolver;

import org.apache.tajo.algebra.ColumnReferenceExpr;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.exception.NoSuchColumnException;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class ResolverByLegacy extends NameResolver {
  @Override
  public Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
      throws PlanningException {

    if (columnRef.hasQualifier()) {
      return resolveColumnWithQualifier(plan, block, columnRef);
    } else {
      return resolveColumnWithoutQualifier(plan, block, columnRef);
    }
  }

  private static Column resolveColumnWithQualifier(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                   ColumnReferenceExpr columnRef) throws PlanningException {
    final String qualifier;
    String canonicalName;
    final String qualifiedName;

    Pair<String, String> normalized = normalizeQualifierAndCanonicalName(block, columnRef);
    qualifier = normalized.getFirst();
    canonicalName = normalized.getSecond();
    qualifiedName = CatalogUtil.buildFQName(qualifier, columnRef.getName());

    Column found = resolveFromRelsWithinBlock(plan, block, columnRef);
    if (found == null) {
      throw new NoSuchColumnException(columnRef.getCanonicalName());
    }

    // If code reach here, a column is found.
    // But, it may be aliased from bottom logical node.
    // If the column is aliased, the found name may not be used in upper node.

    // Here, we try to check if column reference is already aliased.
    // If so, it replaces the name with aliased name.
    LogicalNode currentNode = block.getCurrentNode();

    // The condition (currentNode.getInSchema().contains(column)) means
    // the column can be used at the current node. So, we don't need to find aliase name.
    Schema currentNodeSchema = null;
    if (currentNode != null) {
      if (currentNode instanceof RelationNode) {
        currentNodeSchema = ((RelationNode) currentNode).getTableSchema();
      } else {
        currentNodeSchema = currentNode.getInSchema();
      }
    }

    if (currentNode != null && !currentNodeSchema.contains(found)
        && currentNode.getType() != NodeType.TABLE_SUBQUERY) {
      List<Column> candidates = TUtil.newList();
      if (block.getNamedExprsManager().isAliased(qualifiedName)) {
        String alias = block.getNamedExprsManager().getAlias(canonicalName);
        found = resolve(plan, block, new ColumnReferenceExpr(alias), NameResolvingMode.LEGACY);
        if (found != null) {
          candidates.add(found);
        }
      }
      if (!candidates.isEmpty()) {
        return ensureUniqueColumn(candidates);
      }
    }

    return found;
  }

  static Column resolveColumnWithoutQualifier(LogicalPlan plan, LogicalPlan.QueryBlock block,
                                                     ColumnReferenceExpr columnRef)throws PlanningException {

    Column found = resolveFromAllRelsInBlock(block, columnRef);
    if (found != null) {
      return found;
    }

    found = resolveAliasedName(block, columnRef);
    if (found != null) {
      return found;
    }

    found = resolveFromCurrentAndChildNode(block, columnRef);
    if (found != null) {
      return found;
    }

    found = resolveFromAllRelsInAllBlocks(plan, columnRef);
    if (found != null) {
      return found;
    }

    throw new NoSuchColumnException("ERROR: no such a column name "+ columnRef.getCanonicalName());
  }
}
