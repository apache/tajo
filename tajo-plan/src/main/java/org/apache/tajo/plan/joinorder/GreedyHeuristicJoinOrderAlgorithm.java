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

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.util.Pair;
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
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraph joinGraph)
      throws PlanningException {

    Set<JoinVertex> vertexes = TUtil.newHashSet();
    for (RelationNode relationNode : block.getRelations()) {
      vertexes.add(new RelationVertex(relationNode));
    }
    JoinEdgeFinderContext context = new JoinEdgeFinderContext();
    while (vertexes.size() > 1) {
      JoinEdge bestPair = getBestPair(context,  )
    }



//    // Setup a remain relation set to be joined
//    // Why we should use LinkedHashSet? - it should keep the deterministic for the order of joins.
//    // Otherwise, join orders can be different even if join costs are the same to each other.
//    Set<LogicalNode> remainRelations = new LinkedHashSet<LogicalNode>();
//    for (RelationNode relation : block.getRelations()) {
//      remainRelations.add(relation);
//    }
//
//    LogicalNode latestJoin;
//    JoinEdge bestPair;
//
//    while (remainRelations.size() > 1) {
//      Set<LogicalNode> checkingRelations = new LinkedHashSet<LogicalNode>();
//
//      for (LogicalNode relation : remainRelations) {
//        Collection <String> relationStrings = PlannerUtil.getRelationLineageWithinQueryBlock(plan, relation);
//        List<JoinEdge> joinEdges = new ArrayList<JoinEdge>();
//        String relationCollection = TUtil.collectionToString(relationStrings, ",");
//        List<JoinEdge> joinEdgesForGiven = joinGraph.getIncomingEdges(relationCollection);
//        if (joinEdgesForGiven != null) {
//          joinEdges.addAll(joinEdgesForGiven);
//        }
//        if (relationStrings.size() > 1) {
//          for (String relationString: relationStrings) {
//            joinEdgesForGiven = joinGraph.getIncomingEdges(relationString);
//            if (joinEdgesForGiven != null) {
//              joinEdges.addAll(joinEdgesForGiven);
//            }
//          }
//        }
//
//        // check if the relation is the last piece of outer join
//        boolean endInnerRelation = false;
//        for (JoinEdge joinEdge: joinEdges) {
//          switch(joinEdge.getJoinType()) {
//            case LEFT_ANTI:
//            case RIGHT_ANTI:
//            case LEFT_SEMI:
//            case RIGHT_SEMI:
//            case LEFT_OUTER:
//            case RIGHT_OUTER:
//            case FULL_OUTER:
//              endInnerRelation = true;
//              if (checkingRelations.size() <= 1) {
//                checkingRelations.add(relation);
//              }
//              break;
//          }
//        }
//
//        if (endInnerRelation) {
//          break;
//        }
//
//        checkingRelations.add(relation);
//      }
//
//      remainRelations.removeAll(checkingRelations);
//
//      // Find the best join pair among all joinable operators in candidate set.
//      while (checkingRelations.size() > 1) {
//        LinkedHashSet<String[]> removingJoinEdges = new LinkedHashSet<String[]>();
//        bestPair = getBestPair(plan, joinGraph, checkingRelations, removingJoinEdges);
//
//        checkingRelations.remove(bestPair.getLeftRelation());
//        checkingRelations.remove(bestPair.getRightRelation());
//        for (String[] joinEdge: removingJoinEdges) {
//          // remove the edge of the best pair from join graph
//          joinGraph.removeEdge(joinEdge[0], joinEdge[1]);
//        }
//
//        latestJoin = createJoinNode(plan, bestPair);
//        checkingRelations.add(latestJoin);
//
//        // all logical nodes should be registered to corresponding blocks
//        block.registerNode(latestJoin);
//      }
//
//      // new Logical block should be the first entry of new Set
//      checkingRelations.addAll(remainRelations);
//      remainRelations = checkingRelations;
//    }
//
//    JoinNode joinTree = (JoinNode) remainRelations.iterator().next();
//    // all generated nodes should be registered to corresponding blocks
//    block.registerNode(joinTree);
//    return new FoundJoinOrder(joinTree, getCost(joinTree));
    return null;
  }

//  private static JoinNode createJoinNode(LogicalPlan plan, JoinEdge joinEdge) {
//    LogicalNode left = joinEdge.getLeftRelation();
//    LogicalNode right = joinEdge.getRightRelation();
//
//    JoinNode joinNode = plan.createNode(JoinNode.class);
//
//    if (PlannerUtil.isCommutativeJoin(joinEdge.getJoinType())) {
//      // if only one operator is relation
//      if ((left instanceof RelationNode) && !(right instanceof RelationNode)) {
//        // for left deep
//        joinNode.init(joinEdge.getJoinType(), right, left);
//      } else {
//        // if both operators are relation or if both are relations
//        // we don't need to concern the left-right position.
//        joinNode.init(joinEdge.getJoinType(), left, right);
//      }
//    } else {
//      joinNode.init(joinEdge.getJoinType(), left, right);
//    }
//
//    Schema mergedSchema = SchemaUtil.merge(joinNode.getLeftChild().getOutSchema(),
//        joinNode.getRightChild().getOutSchema());
//    joinNode.setInSchema(mergedSchema);
//    joinNode.setOutSchema(mergedSchema);
//    if (joinEdge.hasJoinQual()) {
//      joinNode.setJoinQual(AlgebraicUtil.createSingletonExprFromCNF(joinEdge.getJoinQual()));
//    }
//    return joinNode;
//  }

  /**
   * Find the best join pair among all joinable operators in candidate set.
   *
   * @param context
   * @param joinGraph a join graph which consists of vertices and edges, where vertex is relation and
   *                  each edge is join condition.
   * @param vertexes candidate operators to be joined.
   * @return The best join pair among them
   * @throws PlanningException
   */
  private JoinEdge getBestPair(JoinEdgeFinderContext context, JoinGraph joinGraph, Set<JoinVertex> vertexes)
      throws PlanningException {
    double minCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;

    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestNonCrossJoin = null;

    for (JoinVertex outer : vertexes) {
      for (JoinVertex inner : vertexes) {
        if (outer.equals(inner)) {
          continue;
        }

        context.reset();
        JoinEdge foundJoin = findJoin(context, joinGraph, outer, outer, inner);
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

  private static class JoinEdgeFinderContext {
    private Set<JoinVertex> visited = TUtil.newHashSet();

    public void reset() {
      visited.clear();
    }
  }

  /**
   * Find a join between two logical operator trees
   *
   * @return If there is no join condition between two relation, it returns NULL value.
   */
  private static JoinEdge findJoin(final JoinEdgeFinderContext context, final JoinGraph joinGraph, JoinVertex begin,
                                   final JoinVertex leftTarget, final JoinVertex rightTarget)
      throws PlanningException {


  }

  private static JoinEdge findJoin(final JoinEdgeFinderContext context, final JoinGraph joinGraph, RelationVertex begin,
                                   final RelationVertex leftTarget, final RelationVertex rightTarget) {
    if (isEqualsOrCommutative(joinGraph, begin, leftTarget)) {
      for (JoinEdge eachEdge : joinGraph.getOutgoingEdges(leftTarget)) {
        if (isEqualsOrCommutative(joinGraph, eachEdge.getRightVertex(), rightTarget)) {
          return eachEdge;
        }
      }
      // not found
      return null;
    } else {
      for (JoinVertex commutativeVertex : getAllCommutativeVertexes(joinGraph, begin)) {
        for (Pair<JoinEdge,JoinEdge> associativePair : getAssociativeEdges(context, joinGraph, commutativeVertex)) {
          // every left edge is already checked above
          JoinEdge rightEdge = associativePair.getSecond();
          JoinEdge found = findJoin(context, joinGraph, rightEdge.getLeftVertex(), leftTarget, rightTarget);
          if (found != null) {
            return found;
          }
        }
      }
      // not found
      return null;
    }
  }

  public static boolean isEqualsOrCommutative(JoinGraph graph, RelationVertex v1, RelationVertex v2) {
    if (v1.equals(v2) || isCommutative(graph, v1, v2)) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean isCommutative(JoinGraph graph, RelationVertex v1, RelationVertex v2) {
    JoinEdge edge = graph.getEdge(v1, v2);
    if (edge != null && PlannerUtil.isCommutativeJoin(edge.getJoinType())) {
      return true;
    }
    return false;
  }

  public static Set<JoinVertex> getAllCommutativeVertexes(JoinGraph graph, RelationVertex from) {
    Set<JoinVertex> founds = TUtil.newHashSet();
    founds.add(from);
    getAllCommutativeVertexes(founds, graph, from);
    return founds;
  }

  private static void getAllCommutativeVertexes(Set<JoinVertex> founds, JoinGraph graph,
                                                JoinVertex vertex) {
    for (JoinEdge candidateEdge : graph.getOutgoingEdges(vertex)) {
      if (PlannerUtil.isCommutativeJoin(candidateEdge.getJoinType())) {
        founds.add(candidateEdge.getRightVertex());
        getAllCommutativeVertexes(founds, tree, candidateEdge.getRightVertex());
      }
    }
  }

  public static Set<Pair<JoinEdge,JoinEdge>> getAssociativeEdges(JoinEdgeFinderContext context, JoinTree tree,
                                                          JoinVertex start) {
    Set<Pair<JoinEdge, JoinEdge>> associativeEdges = TUtil.newHashSet();
    // find all associative edge pairs which begin at the start vertex
    for (JoinEdge leftEdge : tree.getOutgoingEdges(start)) {
      JoinVertex willBeVisited = leftEdge.getRightVertex();
      if (!context.visited.contains(willBeVisited)) {
        for (JoinEdge rightEdge : tree.getOutgoingEdges(willBeVisited)) {
          if (JoinOrderingUtil.isAssociativeJoin(leftEdge.getJoinType(), rightEdge.getJoinType())) {
            associativeEdges.add(new Pair<JoinEdge, JoinEdge>(leftEdge, rightEdge));
          }
        }
      }
    }
    return associativeEdges;
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
      filterFactor = filterFactor * Math.pow(DEFAULT_SELECTION_FACTOR, joinEdge.getJoinQual().size());
      return getCost(joinEdge.getLeftVertex().getCorrespondingNode()) *
          getCost(joinEdge.getRightVertex().getCorrespondingNode()) * filterFactor;
    } else {
      // make cost bigger if cross join
      return Math.pow(getCost(joinEdge.getLeftVertex().getCorrespondingNode()) *
          getCost(joinEdge.getRightVertex().getCorrespondingNode()), 2);
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