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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This is a greedy heuristic algorithm to find a bushy join tree. This algorithm finds
 * the best join order with join conditions and pushed-down join conditions to
 * all join operators.
 */
public class GreedyHeuristicJoinOrderAlgorithm implements JoinOrderAlgorithm {

  public static final double DEFAULT_SELECTION_FACTOR = 0.1;
  private static final Log LOG = LogFactory.getLog(GreedyHeuristicJoinOrderAlgorithm.class);

  @Override
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraphContext graphContext)
      throws PlanningException {

    Set<JoinVertex> vertexes = TUtil.newHashSet();
    for (RelationNode relationNode : block.getRelations()) {
      vertexes.add(new RelationVertex(relationNode));
    }
    JoinEdgeFinderContext context = new JoinEdgeFinderContext();
    JoinGraph joinGraph = graphContext.getJoinGraph();
    while (vertexes.size() > 1) {
      JoinEdge bestPair = getBestPair(context, graphContext, vertexes);
      JoinedRelationsVertex newVertex = new JoinedRelationsVertex(bestPair);

      if (bestPair.getLeftVertex().equals(graphContext.getMostLeftVertex())
          || (PlannerUtil.isCommutativeJoin(bestPair.getJoinType())
          && bestPair.getRightVertex().equals(graphContext.getMostLeftVertex()))) {
        graphContext.setMostLeftVertex(newVertex);
      }

      Set<JoinEdge> willBeRemoved = TUtil.newHashSet();
      Set<JoinEdge> willBeAdded = TUtil.newHashSet();

      prepareGraphUpdate(graphContext, joinGraph, bestPair, newVertex, willBeAdded, willBeRemoved);

      for (JoinEdge edge : willBeRemoved) {
        joinGraph.removeEdge(edge.getLeftVertex(), edge.getRightVertex());
        graphContext.addCandidateJoinConditions(edge.getJoinQual());
      }

      for (JoinEdge edge : willBeAdded) {
        joinGraph.addEdge(edge.getLeftVertex(), edge.getRightVertex(), edge);
        graphContext.removeCandidateJoinConditions(edge.getJoinQual());
        graphContext.removeCandidateJoinFilters(edge.getJoinQual());
      }

      graphContext.removeCandidateJoinConditions(bestPair.getJoinQual());
      graphContext.removeCandidateJoinFilters(bestPair.getJoinQual());

      vertexes.remove(bestPair.getLeftVertex());
      vertexes.remove(bestPair.getRightVertex());
      vertexes.add(newVertex);
    }

    JoinNode joinTree = (JoinNode) vertexes.iterator().next().buildPlan(plan, block);
    // all generated nodes should be registered to corresponding blocks
    block.registerNode(joinTree);
    return new FoundJoinOrder(joinTree, getCost(joinTree));
  }

  private void prepareGraphUpdate(JoinGraphContext context, JoinGraph graph, JoinEdge bestPair,
                                  JoinedRelationsVertex vertex, Set<JoinEdge> willBeAdded, Set<JoinEdge> willBeRemoved) {
    prepareGraphUpdate(context, graph.getOutgoingEdges(bestPair.getLeftVertex()), vertex, true,
        willBeAdded, willBeRemoved);

    prepareGraphUpdate(context, graph.getIncomingEdges(bestPair.getLeftVertex()), vertex, false,
        willBeAdded, willBeRemoved);

    prepareGraphUpdate(context, graph.getOutgoingEdges(bestPair.getRightVertex()), vertex, true,
        willBeAdded, willBeRemoved);

    prepareGraphUpdate(context, graph.getIncomingEdges(bestPair.getRightVertex()), vertex, false,
        willBeAdded, willBeRemoved);
  }

  private void prepareGraphUpdate(JoinGraphContext context, List<JoinEdge> edges,
                                  JoinedRelationsVertex vertex, boolean isLeftVertex,
                                  Set<JoinEdge> willBeAdded, Set<JoinEdge> willBeRemoved) {
    if (edges != null) {
      for (JoinEdge edge : edges) {
        if (!JoinOrderingUtil.isEqualsOrSymmetric(vertex.getJoinEdge(), edge)) {
          if (isLeftVertex) {
            willBeAdded.add(context.getCachedOrNewJoinEdge(edge.getJoinSpec(), vertex, edge.getRightVertex()));
          } else {
            willBeAdded.add(context.getCachedOrNewJoinEdge(edge.getJoinSpec(), edge.getLeftVertex(), vertex));
          }
        }
        willBeRemoved.add(edge);
      }
    }
  }

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
  private JoinEdge getBestPair(JoinEdgeFinderContext context, JoinGraphContext graphContext, Set<JoinVertex> vertexes)
      throws PlanningException {
    double minCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;

    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestNonCrossJoin = null;

    LOG.info("outer");
    for (JoinVertex vertex : vertexes) {

    }

    for (JoinVertex outer : vertexes) {
      for (JoinVertex inner : vertexes) {
        if (outer.equals(inner)) {
          continue;
        }

        context.reset();
        JoinEdge foundJoin = findJoin(context, graphContext, graphContext.getMostLeftVertex(), outer, inner);
        if (foundJoin == null) {
          LOG.error("Join between (" + outer + ", " + inner + ") is not found.");
          continue;
        }
        Set<EvalNode> additionalPredicates = JoinOrderingUtil.findJoinConditionForJoinVertex(
            graphContext.getCandidateJoinConditions(), foundJoin, true);
        additionalPredicates.addAll(JoinOrderingUtil.findJoinConditionForJoinVertex(
            graphContext.getCandidateJoinFilters(), foundJoin, false));
        foundJoin = JoinOrderingUtil.addPredicates(foundJoin, additionalPredicates);
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
      if (bestNonCrossJoin.hasJoinQual()) {
        graphContext.removeCandidateJoinFilters(bestNonCrossJoin.getJoinQual());
      }
      return bestNonCrossJoin;
    } else {
      if (bestJoin.hasJoinQual()) {
        graphContext.removeCandidateJoinFilters(bestJoin.getJoinQual());
      }
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
  private static JoinEdge findJoin(final JoinEdgeFinderContext context, final JoinGraphContext graphContext,
                                   JoinVertex begin, final JoinVertex leftTarget, final JoinVertex rightTarget)
      throws PlanningException {

    context.visited.add(begin);

    JoinGraph joinGraph = graphContext.getJoinGraph();

    // Find the matching edge from begin
    Set<JoinVertex> interchangeableWithBegin = JoinOrderingUtil.getAllInterchangeableVertexes(graphContext, begin);

    if (interchangeableWithBegin.contains(leftTarget)) {
      List<JoinEdge> edgesFromLeftTarget = joinGraph.getOutgoingEdges(leftTarget);
      if (edgesFromLeftTarget != null) {
        for (JoinEdge edgeFromLeftTarget : edgesFromLeftTarget) {
          edgeFromLeftTarget = JoinOrderingUtil.updateQualIfNecessary(graphContext, edgeFromLeftTarget);
          Set<JoinVertex> interchangeableWithRightVertex;
          if (edgeFromLeftTarget.getJoinType() == JoinType.INNER || edgeFromLeftTarget.getJoinType() == JoinType.CROSS) {
            interchangeableWithRightVertex = JoinOrderingUtil.getAllInterchangeableVertexes(graphContext,
                edgeFromLeftTarget.getRightVertex());
          } else {
            interchangeableWithRightVertex = TUtil.newHashSet(edgeFromLeftTarget.getRightVertex());
          }

          if (interchangeableWithRightVertex.contains(rightTarget)) {
            JoinEdge targetEdge = joinGraph.getEdge(leftTarget, rightTarget);
            if (targetEdge == null) {
              // Since the targets of the both sides are searched with symmetric characteristics,
              // the join type is assumed as CROSS.
              joinGraph.addJoin(graphContext, new JoinSpec(JoinType.CROSS), leftTarget, rightTarget);
              return JoinOrderingUtil.updateQualIfNecessary(graphContext, joinGraph.getEdge(leftTarget, rightTarget));
            } else {
              targetEdge = JoinOrderingUtil.updateQualIfNecessary(graphContext, targetEdge);
              return targetEdge;
            }
          }
        }
      }
    }

    for (JoinVertex interchangeableVertex : interchangeableWithBegin) {
        List<JoinEdge> edges = joinGraph.getOutgoingEdges(interchangeableVertex);
        if (edges != null) {
          for (JoinEdge edge : edges) {
            for (JoinEdge associativeEdge : JoinOrderingUtil.getAllAssociativeEdges(graphContext, edge)) {
              JoinVertex willBeVisited = associativeEdge.getLeftVertex();
              if (!context.visited.contains(willBeVisited)) {
                JoinEdge found = findJoin(context, graphContext, associativeEdge.getLeftVertex(), leftTarget,
                    rightTarget);
                if (found != null) {
                  return found;
                }
              }
            }
          }
        }
      }
      // not found
      return null;
//    }
  }

//  private static JoinEdge updateQualIfNecessary(JoinGraphContext context, JoinEdge edge) {
//    Set<EvalNode> additionalPredicates = JoinOrderingUtil.findJoinConditionForJoinVertex(
//        context.getCandidateJoinConditions(), edge, true);
//    additionalPredicates.addAll(JoinOrderingUtil.findJoinConditionForJoinVertex(
//        context.getCandidateJoinFilters(), edge, false));
////    context.getCandidateJoinConditions().removeAll(additionalPredicates);
////    context.getCandidateJoinFilters().removeAll(additionalPredicates);
////    return JoinOrderingUtil.addPredicates(edge, additionalPredicates);
//    edge.addJoinPredicates(additionalPredicates);
//    return edge;
//  }
//
//  /**
//   * Find all edges that are associative with the given edge.
//   *
//   * @param context
//   * @param edge
//   * @return
//   */
//  private static Set<JoinEdge> getAllAssociativeEdges(LogicalPlan plan, JoinGraphContext context, JoinEdge edge) {
//    Set<JoinEdge> associativeEdges = TUtil.newHashSet();
//    JoinVertex start = edge.getRightVertex();
//    List<JoinEdge> candidateEdges = context.getJoinGraph().getOutgoingEdges(start);
//    if (candidateEdges != null) {
//      for (JoinEdge candidateEdge : candidateEdges) {
//        candidateEdge = updateQualIfNecessary(context, candidateEdge);
//        if (!isEqualsOrSymmetric(edge, candidateEdge) &&
//            JoinOrderingUtil.isAssociativeJoin(context, edge, candidateEdge)) {
//          associativeEdges.add(candidateEdge);
//        }
//      }
//    }
//    return associativeEdges;
//  }
//
//  private static boolean isEqualsOrSymmetric(JoinEdge edge1, JoinEdge edge2) {
//    if (edge1.equals(edge2) || isCommutative(edge1, edge2)) {
//      return true;
//    }
//    return false;
//  }
//
//  private static boolean isCommutative(JoinEdge edge1, JoinEdge edge2) {
//    if (edge1.getLeftVertex().equals(edge2.getRightVertex()) &&
//        edge1.getRightVertex().equals(edge2.getLeftVertex()) &&
//        edge1.getJoinSpec().equals(edge2.getJoinSpec()) &&
//        PlannerUtil.isCommutativeJoin(edge1.getJoinType())) {
//      return true;
//    }
//    return false;
//  }
//
//  private static Set<JoinVertex> getAllInterchangeableVertexes(LogicalPlan plan, JoinGraphContext context, JoinVertex from) {
//    Set<JoinVertex> founds = TUtil.newHashSet();
//    getAllInterchangeableVertexes(founds, plan, context, from);
//    return founds;
//  }
//
//  private static void getAllInterchangeableVertexes(Set<JoinVertex> founds, LogicalPlan plan, JoinGraphContext context,
//                                                    JoinVertex vertex) {
//    founds.add(vertex);
//    Set<JoinVertex> foundAtThis = TUtil.newHashSet();
//    List<JoinEdge> candidateEdges = context.getJoinGraph().getOutgoingEdges(vertex);
//    if (candidateEdges != null) {
//      for (JoinEdge candidateEdge : candidateEdges) {
//        candidateEdge = updateQualIfNecessary(context, candidateEdge);
//        if (PlannerUtil.isCommutativeJoin(candidateEdge.getJoinType())
//            && !founds.contains(candidateEdge.getRightVertex())) {
//          List<JoinEdge> rightEdgesOfCandidate = context.getJoinGraph().getOutgoingEdges(candidateEdge.getRightVertex());
//          boolean reacheable = true;
//          if (rightEdgesOfCandidate != null) {
//            for (JoinEdge rightEdgeOfCandidate : rightEdgesOfCandidate) {
//              rightEdgeOfCandidate = updateQualIfNecessary(context, rightEdgeOfCandidate);
////              if (!PlannerUtil.isCommutativeJoin(rightEdgeOfCandidate.getJoinType())) {
//              if (!isCommutative(candidateEdge, rightEdgeOfCandidate) &&
//                  !JoinOrderingUtil.isAssociativeJoin(context, candidateEdge, rightEdgeOfCandidate)) {
//                reacheable = false;
//                break;
//              }
//            }
//          }
//          if (reacheable) {
//            foundAtThis.add(candidateEdge.getRightVertex());
//          }
//        }
//      }
//      if (foundAtThis.size() > 0) {
////        founds.addAll(foundAtThis);
//        for (JoinVertex v : foundAtThis) {
//          getAllInterchangeableVertexes(founds, plan, context, v);
//        }
//      }
//    }
//  }

  /**
   * Getting a cost of one join
   * @param joinEdge
   * @return
   */
  public static double getCost(JoinEdge joinEdge) {
    double filterFactor = 1;
    double cost;
    if (joinEdge.getJoinType() != JoinType.CROSS) {
      // TODO - should consider join type
      // TODO - should statistic information obtained from query history
      filterFactor = filterFactor * Math.pow(DEFAULT_SELECTION_FACTOR, joinEdge.getJoinQual().size());
//      return getCost(joinEdge.getLeftVertex()) *
//          getCost(joinEdge.getRightVertex()) * filterFactor;
      cost = getCost(joinEdge.getLeftVertex()) *
          getCost(joinEdge.getRightVertex()) * filterFactor;
    } else {
      // make cost bigger if cross join
//      return Math.pow(getCost(joinEdge.getLeftVertex()) *
//          getCost(joinEdge.getRightVertex()), 2);
      cost = Math.pow(getCost(joinEdge.getLeftVertex()) *
          getCost(joinEdge.getRightVertex()), 2);
    }

    LOG.info("cost of " + joinEdge + " : " + cost);
    return cost;
  }

  public static double getCost(JoinVertex joinVertex) {
    double cost;
    if (joinVertex instanceof RelationVertex) {
//      return getCost(((RelationVertex) joinVertex).getRelationNode());
      cost = getCost(((RelationVertex) joinVertex).getRelationNode());
    } else {
//      return getCost(((JoinedRelationsVertex)joinVertex).getJoinEdge());
      cost = getCost(((JoinedRelationsVertex)joinVertex).getJoinEdge());
    }
    LOG.info("cost of " + joinVertex + " : " + cost);
    return cost;
  }

  // TODO - costs of other operator operators (e.g., group-by and sort) should be computed in proper manners.
  public static double getCost(LogicalNode node) {
    double cost;
    switch (node.getType()) {

    case PROJECTION:
      ProjectionNode projectionNode = (ProjectionNode) node;
//      return getCost(projectionNode.getChild());
      cost = getCost(projectionNode.getChild());
      break;

    case JOIN:
      JoinNode joinNode = (JoinNode) node;
      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        filterFactor = Math.pow(DEFAULT_SELECTION_FACTOR,
            AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual()).length);
//        return getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()) * filterFactor;
        cost = getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()) * filterFactor;
      } else {
//        return Math.pow(getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()), 2);
        cost = Math.pow(getCost(joinNode.getLeftChild()) * getCost(joinNode.getRightChild()), 2);
      }
      break;

    case SELECTION:
      SelectionNode selectionNode = (SelectionNode) node;
//      return getCost(selectionNode.getChild()) *
//          Math.pow(DEFAULT_SELECTION_FACTOR, AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual()).length);
      cost = getCost(selectionNode.getChild()) *
          Math.pow(DEFAULT_SELECTION_FACTOR, AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual()).length);
      break;

    case TABLE_SUBQUERY:
      TableSubQueryNode subQueryNode = (TableSubQueryNode) node;
//      return getCost(subQueryNode.getSubQuery());
      cost = getCost(subQueryNode.getSubQuery());
      break;

    case SCAN:
      ScanNode scanNode = (ScanNode) node;
      if (scanNode.getTableDesc().getStats() != null) {
        cost = ((ScanNode)node).getTableDesc().getStats().getNumBytes();
//        return cost;
      } else {
//        return Long.MAX_VALUE;
        cost = Long.MAX_VALUE;
      }
      break;

    case UNION:
      UnionNode unionNode = (UnionNode) node;
//      return getCost(unionNode.getLeftChild()) + getCost(unionNode.getRightChild());
      cost = getCost(unionNode.getLeftChild()) + getCost(unionNode.getRightChild());
      break;

    case EXCEPT:
    case INTERSECT:
      throw new UnsupportedOperationException("getCost() does not support EXCEPT or INTERSECT yet");

    default:
      // all binary operators (join, union, except, and intersect) are handled in the above cases.
      // So, we need to handle only unary nodes in default.
//      return getCost(((UnaryNode) node).getChild());
      cost = getCost(((UnaryNode) node).getChild());
      break;
    }

    LOG.info("cost of " + node + " : " + cost);
    return cost;
  }
}