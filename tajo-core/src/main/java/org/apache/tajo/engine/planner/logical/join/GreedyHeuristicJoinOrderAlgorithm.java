/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.logical.join;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.AlgebraicUtil;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.*;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This is a greedy heuristic algorithm to find a bushy join tree. This algorithm finds
 * the best join order with join conditions and pushed-down join conditions to
 * all join operators.
 */
public class GreedyHeuristicJoinOrderAlgorithm implements JoinOrderAlgorithm {
  public static double DEFAULT_SELECTION_FACTOR = 0.1;

  @Override
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraph joinGraph,
                                      Set<String> relationsWithoutQual) throws PlanningException {

    // Setup a remain relation set to be joined
    // Why we should use LinkedHashSet? - it should keep the deterministic for the order of joins.
    // Otherwise, join orders can be different even if join costs are the same to each other.
    Set<LogicalNode> remainRelations = new LinkedHashSet<LogicalNode>();
    for (RelationNode relation : block.getRelations()) {
      remainRelations.add(relation);
    }

    LogicalNode latestJoin;
    JoinEdge bestPair;

    while (remainRelations.size() > 1) {
      // Find the best join pair among all joinable operators in candidate set.
      bestPair = getBestPair(plan, joinGraph, remainRelations);

      remainRelations.remove(bestPair.getLeftRelation()); // remainRels = remainRels \ Ti
      remainRelations.remove(bestPair.getRightRelation()); // remainRels = remainRels \ Tj

      latestJoin = createJoinNode(plan, bestPair);
      remainRelations.add(latestJoin);

      // all logical nodes should be registered to corresponding blocks
      block.registerNode(latestJoin);
    }

    JoinNode joinTree = (JoinNode) remainRelations.iterator().next();
    // all generated nodes should be registered to corresponding blocks
    block.registerNode(joinTree);
    return new FoundJoinOrder(joinTree, getCost(joinTree));
  }

  private static JoinNode createJoinNode(LogicalPlan plan, JoinEdge joinEdge) {
    LogicalNode left = joinEdge.getLeftRelation();
    LogicalNode right = joinEdge.getRightRelation();

    JoinNode joinNode = plan.createNode(JoinNode.class);

    if (PlannerUtil.isCommutativeJoin(joinEdge.getJoinType())) {
      // if only one operator is relation
      if ((left instanceof RelationNode) && !(right instanceof RelationNode)) {
        // for left deep
        joinNode.init(joinEdge.getJoinType(), right, left);
      } else {
        // if both operators are relation or if both are relations
        // we don't need to concern the left-right position.
        joinNode.init(joinEdge.getJoinType(), left, right);
      }
    } else {
      joinNode.init(joinEdge.getJoinType(), left, right);
    }

    Schema mergedSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
        joinNode.getRightChild().getOutSchema());
    joinNode.setInSchema(mergedSchema);
    joinNode.setOutSchema(mergedSchema);
    if (joinEdge.hasJoinQual()) {
      joinNode.setJoinQual(AlgebraicUtil.createSingletonExprFromCNF(joinEdge.getJoinQual()));
    }
    return joinNode;
  }

  /**
   * Find the best join pair among all joinable operators in candidate set.
   *
   * @param plan a logical plan
   * @param graph a join graph which consists of vertices and edges, where vertex is relation and
   *              each edge is join condition.
   * @param candidateSet candidate operators to be joined.
   * @return The best join pair among them
   * @throws PlanningException
   */
  private JoinEdge getBestPair(LogicalPlan plan, JoinGraph graph, Set<LogicalNode> candidateSet)
      throws PlanningException {
    double minCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;

    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestNonCrossJoin = null;

    for (LogicalNode outer : candidateSet) {
      for (LogicalNode inner : candidateSet) {
        if (outer.equals(inner)) {
          continue;
        }

        JoinEdge foundJoin = findJoin(plan, graph, outer, inner);
        if (foundJoin == null) {
          continue;
        }
        double cost = getCost(foundJoin);

        if (cost < minCost) {
          minCost = cost;
          bestJoin = foundJoin;
        }

        // Keep the min cost join
        // But, if there exists a qualified join, the qualified join must be chosen
        // rather than cross join regardless of cost.
        if (foundJoin.hasJoinQual()) {
          if (cost < minNonCrossJoinCost) {
            minNonCrossJoinCost = cost;
            bestNonCrossJoin = foundJoin;
          }
        }
      }
    }

    if (bestNonCrossJoin != null) {
      return bestNonCrossJoin;
    } else {
      return bestJoin;
    }
  }

  /**
   * Find a join between two logical operator trees
   *
   * @return If there is no join condition between two relation, it returns NULL value.
   */
  private static JoinEdge findJoin(LogicalPlan plan, JoinGraph graph, LogicalNode outer, LogicalNode inner)
      throws PlanningException {
    JoinEdge foundJoinEdge = null;

    // If outer is outer join, make edge key using all relation names in outer.
    SortedSet<String> relationNames =
        new TreeSet<String>(PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer));
    String outerEdgeKey = TUtil.collectionToString(relationNames);
    for (String innerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner)) {
      if (graph.hasEdge(outerEdgeKey, innerName)) {
        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, innerName);
        if (foundJoinEdge == null) {
          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
              existJoinEdge.getJoinQual());
        } else {
          foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
              existJoinEdge.getJoinQual()));
        }
      }
    }
    if (foundJoinEdge != null) {
      return foundJoinEdge;
    }

    relationNames =
        new TreeSet<String>(PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner));
    outerEdgeKey = TUtil.collectionToString(relationNames);
    for (String outerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer)) {
      if (graph.hasEdge(outerEdgeKey, outerName)) {
        JoinEdge existJoinEdge = graph.getEdge(outerEdgeKey, outerName);
        if (foundJoinEdge == null) {
          foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), inner, outer,
              existJoinEdge.getJoinQual());
        } else {
          foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
              existJoinEdge.getJoinQual()));
        }
      }
    }
    if (foundJoinEdge != null) {
      return foundJoinEdge;
    }

    for (String outerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, outer)) {
      for (String innerName : PlannerUtil.getRelationLineageWithinQueryBlock(plan, inner)) {

        // Find all joins between two relations and merge them into one join if possible
        if (graph.hasEdge(outerName, innerName)) {
          JoinEdge existJoinEdge = graph.getEdge(outerName, innerName);
          if (foundJoinEdge == null) {
            foundJoinEdge = new JoinEdge(existJoinEdge.getJoinType(), outer, inner,
                existJoinEdge.getJoinQual());
          } else {
            foundJoinEdge.addJoinQual(AlgebraicUtil.createSingletonExprFromCNF(
                existJoinEdge.getJoinQual()));
          }
        }
      }
    }

    if (foundJoinEdge == null) {
      foundJoinEdge = new JoinEdge(JoinType.CROSS, outer, inner);
    }

    return foundJoinEdge;
  }

  /**
   * Getting a cost of one join
   * @param joinEdge
   * @return
   */
  public static double getCost(JoinEdge joinEdge) {
    double filterFactor = 1;
    if (joinEdge.hasJoinQual()) {
      // TODO - should consider join type
      // TODO - should statistic information obtained from query history
      filterFactor = filterFactor * Math.pow(DEFAULT_SELECTION_FACTOR, joinEdge.getJoinQual().length);
      return getCost(joinEdge.getLeftRelation()) * getCost(joinEdge.getRightRelation()) * filterFactor;
    } else {
      // make cost bigger if cross join
      return Math.pow(getCost(joinEdge.getLeftRelation()) * getCost(joinEdge.getRightRelation()), 2);
    }
  }

  // TODO - costs of other operator operators (e.g., group-by and sort) should be computed in proper manners.
  public static double getCost(LogicalNode node) {
    switch (node.getType()) {

    case PROJECTION:
      ProjectionNode projectionNode = (ProjectionNode) node;
      return getCost(projectionNode.getChild());

    case JOIN:
      JoinNode joinNode = (JoinNode) node;
      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        filterFactor = Math.pow(DEFAULT_SELECTION_FACTOR,
            AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual()).length);
        return getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()) * filterFactor;
      } else {
        return Math.pow(getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()), 2);
      }

    case SELECTION:
      SelectionNode selectionNode = (SelectionNode) node;
      return getCost(selectionNode.getChild()) *
          Math.pow(DEFAULT_SELECTION_FACTOR, AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual()).length);

    case TABLE_SUBQUERY:
      TableSubQueryNode subQueryNode = (TableSubQueryNode) node;
      return getCost(subQueryNode.getSubQuery());

    case SCAN:
      ScanNode scanNode = (ScanNode) node;
      if (scanNode.getTableDesc().getStats() != null) {
        double cost = ((ScanNode)node).getTableDesc().getStats().getNumBytes();
        return cost;
      } else {
        return Long.MAX_VALUE;
      }

    case UNION:
      UnionNode unionNode = (UnionNode) node;
      return getCost(unionNode.getLeftChild()) + getCost(unionNode.getRightChild());

    case EXCEPT:
    case INTERSECT:
      throw new UnsupportedOperationException("getCost() does not support EXCEPT or INTERSECT yet");

    default:
      // all binary operators (join, union, except, and intersect) are handled in the above cases.
      // So, we need to handle only unary nodes in default.
      return getCost(((UnaryNode) node).getChild());
    }
  }
}