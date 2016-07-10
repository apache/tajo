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
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.expr.AlgebraicUtil;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.StringUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a greedy heuristic algorithm to find a bushy join tree. This algorithm finds
 * the best join order with join conditions and pushed-down join conditions to
 * appropriate join operators.
 */
public class GreedyHeuristicJoinOrderAlgorithm implements JoinOrderAlgorithm {

  public static final double DEFAULT_SELECTION_FACTOR = 0.1;

  @Override
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraphContext graphContext)
      throws TajoException {

    Set<JoinVertex> vertexes = block.getRelations().stream().map(RelationVertex::new).collect(Collectors.toSet());

    // As illustrated at LogicalOptimizer.JoinGraphBuilder, the join graph initially forms a kind of tree.
    // This join graph can be updated by adding new join edges or removing existing join edges
    // during join order optimization.
    JoinEdgeFinderContext context = new JoinEdgeFinderContext();
    JoinGraph joinGraph = graphContext.getJoinGraph();
    while (vertexes.size() > 1) {
      JoinEdge bestPair = getBestPair(context, graphContext, vertexes);
      JoinedRelationsVertex newVertex = new JoinedRelationsVertex(bestPair);

      // A root vertex is the join vertex where the graph traverse is started.
      // The root vertex should be updated if the previous root vertex is merged into a new one.
      if (graphContext.getRootVertexes().contains(bestPair.getLeftVertex())) {
        graphContext.replaceRootVertexes(bestPair.getLeftVertex(), newVertex);
      } else if (PlannerUtil.isCommutativeJoinType(bestPair.getJoinType())
          && graphContext.getRootVertexes().contains(bestPair.getRightVertex())) {
        graphContext.replaceRootVertexes(bestPair.getRightVertex(), newVertex);
      }

      // Once a best pair is chosen, some existing join edges should be removed and new join edges should be added.
      //
      // There can be some join edges which are equal to or symmetric with the best pair.
      // They cannot be chosen anymore, and thus should be removed from the join graph.
      //
      // The chosen best pair will be regarded as a join vertex again.
      // So, the join edges which share any vertexes with the best pair should be updated, too.
      Set<JoinEdge> willBeRemoved = new HashSet<>();
      Set<JoinEdge> willBeAdded = new HashSet<>();

      // Find every join edges which should be updated.
      prepareGraphUpdate(graphContext, joinGraph, bestPair, newVertex, willBeAdded, willBeRemoved);

      // Update the join graph
      updateGraph(graphContext, joinGraph, bestPair, willBeAdded, willBeRemoved);

      // Update the join vertex set
      vertexes.remove(bestPair.getLeftVertex());
      vertexes.remove(bestPair.getRightVertex());
      vertexes.add(newVertex);
    }

    JoinNode joinTree = (JoinNode) vertexes.iterator().next().buildPlan(plan, block);
    // all generated nodes should be registered to corresponding blocks
    block.registerNode(joinTree);
    return new FoundJoinOrder(joinTree, getCost(joinTree));
  }

  private void updateGraph(JoinGraphContext context, JoinGraph graph, JoinEdge bestPair,
                           Set<JoinEdge> willBeAdded, Set<JoinEdge> willBeRemoved) {
    for (JoinEdge edge : willBeRemoved) {
      graph.removeEdge(edge.getLeftVertex(), edge.getRightVertex());
      context.addCandidateJoinConditions(edge.getJoinQual());
    }

    for (JoinEdge edge : willBeAdded) {
      graph.addEdge(edge.getLeftVertex(), edge.getRightVertex(), edge);
      context.removeCandidateJoinConditions(edge.getJoinQual());
      context.removeCandidateJoinFilters(edge.getJoinQual());
    }

    // Join quals involved by the best pair should be removed.
    context.markAsEvaluatedJoinConditions(bestPair.getJoinQual());
    context.markAsEvaluatedJoinFilters(bestPair.getJoinQual());
  }

  /**
   * Once a best pair is found, some existing join edges which are equal to or symmetric with the best pair should be
   * removed and new join edges should be added.
   * They cannot be chosen as the best pair anymore, and thus should be removed from the join graph.
   * This method finds such join edges to prepare updating the join graph.
   *
   * @param context
   * @param graph
   * @param bestPair
   * @param vertex
   * @param willBeAdded
   * @param willBeRemoved
   */
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
   * Find the best join pair among all joinable operators in the candidate set.
   *
   * @param context
   * @param graphContext a join graph which consists of vertices and edges, where vertex is relation and
   *                     each edge is join condition.
   * @param vertexes candidate operators to be joined.
   * @return The best join pair among them
   */
  private JoinEdge getBestPair(JoinEdgeFinderContext context, JoinGraphContext graphContext, Set<JoinVertex> vertexes)
      throws TajoException {
    double minCost = Double.MAX_VALUE;
    JoinEdge bestJoin = null;

    double minNonCrossJoinCost = Double.MAX_VALUE;
    JoinEdge bestNonCrossJoin = null;

    // Brute-force algorithm
    // check every possible combination of join vertexes.
    for (JoinVertex outer : vertexes) {
      for (JoinVertex inner : vertexes) {
        if (outer.equals(inner)) {
          continue;
        }

        context.reset();
        JoinEdge foundJoin = null;

        // A root vertex is the join vertex where the graph traverse is started.
        // For each root vertex, find possible joins between inner and outer join vertexes.
        for (JoinVertex eachRoot : graphContext.getRootVertexes()) {
          foundJoin = findJoin(context, graphContext, eachRoot, outer, inner);
          if (foundJoin != null) break;
        }
        if (foundJoin == null) {
          continue;
        }
        // The found join edge may not have join quals even though they can be evaluated during join.
        // So, possible join quals should be added to the join node before estimating its cost.
        JoinOrderingUtil.updateQualIfNecessary(graphContext, foundJoin);
        double cost = getCost(foundJoin);

        if (cost < minCost ||
            (cost == minCost && cost == Double.MAX_VALUE)) {
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
        graphContext.markAsEvaluatedJoinConditions(bestNonCrossJoin.getJoinQual());
      }
      return swapLeftAndRightIfNecessary(bestNonCrossJoin);
    } else if (bestJoin != null) {
      if (bestJoin.hasJoinQual()) {
        graphContext.markAsEvaluatedJoinFilters(bestJoin.getJoinQual());
      }
      return swapLeftAndRightIfNecessary(bestJoin);
    } else {
      throw new TajoInternalError("Cannot find the best join order");
    }
  }

  private static JoinEdge swapLeftAndRightIfNecessary(JoinEdge edge) {
    if (PlannerUtil.isCommutativeJoinType(edge.getJoinType())) {
      double leftCost = getCostOfVertex(edge.getLeftVertex());
      double rightCost = getCostOfVertex(edge.getRightVertex());
      if (leftCost < rightCost) {
        return new JoinEdge(edge.getJoinSpec(), edge.getRightVertex(), edge.getLeftVertex());
      } else if (leftCost == rightCost) {
        // compare the relation name to make the join order determinant
        if (StringUtils.join(edge.getLeftVertex().getRelations(), "").
            compareTo(StringUtils.join(edge.getRightVertex().getRelations(), "")) < 0) {
          return new JoinEdge(edge.getJoinSpec(), edge.getRightVertex(), edge.getLeftVertex());
        }
      }
    }
    return edge;
  }

  private static class JoinEdgeFinderContext {
    private Set<JoinVertex> visited = new HashSet<>();

    public void reset() {
      visited.clear();
    }
  }

  /**
   * Find a join edge between two join vertexes.
   *
   * @param context context for edge finder
   * @param graphContext graph context
   * @param begin begin vertex to traverse the join graph
   * @param leftTarget left target join vertex
   * @param rightTarget right target join vertex
   * @return If there is no join edge between two vertexes, it returns null.
   */
  private static JoinEdge findJoin(final JoinEdgeFinderContext context, final JoinGraphContext graphContext,
                                   JoinVertex begin, final JoinVertex leftTarget, final JoinVertex rightTarget)
      throws TajoException {

    context.visited.add(begin);

    JoinGraph joinGraph = graphContext.getJoinGraph();

    // Get all interchangeable vertexes of the begin vertex.
    // Please see JoinOrderingUtil.getAllInterchangeableVertexes() for interchangeable vertexes.
    Set<JoinVertex> interchangeableWithBegin = JoinOrderingUtil.getAllInterchangeableVertexes(graphContext, begin);

    // If the left search target is interchangeable with the begin vertex, check every outgoing edges
    // from the left target to find the join edge who has the right search target as its right vertex.
    if (interchangeableWithBegin.contains(leftTarget)) {
      List<JoinEdge> edgesFromLeftTarget = joinGraph.getOutgoingEdges(leftTarget);
      if (edgesFromLeftTarget != null) {
        for (JoinEdge edgeFromLeftTarget : edgesFromLeftTarget) {
          edgeFromLeftTarget = JoinOrderingUtil.updateQualIfNecessary(graphContext, edgeFromLeftTarget);

          // Find all interchangeable vertexes with the right vertex of the current edge.
          // If the right target vertex is interchangeable with the right vertex of the current edge,
          // we've successfully found a join edge between the left and right targets.
          Set<JoinVertex> interchangeableWithRightVertex;
          if (edgeFromLeftTarget.getJoinType() == JoinType.INNER || edgeFromLeftTarget.getJoinType() == JoinType.CROSS) {
            interchangeableWithRightVertex = JoinOrderingUtil.getAllInterchangeableVertexes(graphContext,
                edgeFromLeftTarget.getRightVertex());
          } else {
            interchangeableWithRightVertex = new HashSet<>(Arrays.asList(edgeFromLeftTarget.getRightVertex()));
          }

          if (interchangeableWithRightVertex.contains(rightTarget)) {
            JoinEdge targetEdge = joinGraph.getEdge(leftTarget, rightTarget);
            if (targetEdge == null) {
              if (joinGraph.allowArbitraryCrossJoin()) {
                // Since the targets of the both sides are searched with symmetric characteristics,
                // the join type is assumed as CROSS.
                // TODO: This must be improved to consider a case when a query involves multiple commutative and
                // TODO: non-commutative joins. It will be done at TAJO-1683.
                joinGraph.addJoin(graphContext, new JoinSpec(JoinType.CROSS), leftTarget, rightTarget);
                return JoinOrderingUtil.updateQualIfNecessary(graphContext, joinGraph.getEdge(leftTarget, rightTarget));
              }
            } else {
              targetEdge = JoinOrderingUtil.updateQualIfNecessary(graphContext, targetEdge);
              return targetEdge;
            }
          }
        }
      }
    }

    // If the left search target is NOT interchangeable with the begin vertex,
    // we cannot find any join edges from the current begin vertex, so search from other vertexes.
    // Here, we should consider the associativity to check whether other joins can be executed earlier than the join
    // who has the begin vertex as its left vertex.
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
  }

  // COMPUTATION_FACTOR is used to give the larger cost for longer plans.
  // We assume that every operation has same cost.
  // TODO: more accurate cost estimation is required.
  private static final double COMPUTATION_FACTOR = 1.5;

  /**
   * Getting a cost of one join
   * @param joinEdge
   * @return
   */
  public static double getCost(JoinEdge joinEdge) {
    double factor = 1;
    double cost;
    if (joinEdge.getJoinType() != JoinType.CROSS) {
      // TODO - should statistic information obtained from query history
      switch (joinEdge.getJoinType()) {
        // TODO - improve cost estimation
        // for outer joins, filter factor does not matter
        case LEFT_OUTER:
          factor *= (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema()) /
              (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getLeftVertex().getSchema());
          break;
        case RIGHT_OUTER:
          factor *= (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema()) /
              (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getRightVertex().getSchema());
          break;
        case FULL_OUTER:
          factor *= Math.max((float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema()) /
                  (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getLeftVertex().getSchema()),
              (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema()) /
                  (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getRightVertex().getSchema()));
          break;
        case LEFT_ANTI:
        case LEFT_SEMI:
          factor *= DEFAULT_SELECTION_FACTOR * SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema()) /
              (float)SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getLeftVertex().getSchema());
          break;
        case INNER:
        default:
          // by default, do the same operation with that of inner join
          // filter factor * output tuple width / input tuple width
          factor *= Math.pow(DEFAULT_SELECTION_FACTOR, joinEdge.getJoinQual().size())
              * SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getSchema())
              / (float)(SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getLeftVertex().getSchema())
              + SchemaUtil.estimateRowByteSizeWithSchema(joinEdge.getRightVertex().getSchema()));
          break;
      }
      // cost = estimated input size * filter factor * (output tuple width / input tuple width)
      cost = getCostOfVertex(joinEdge.getLeftVertex()) *
          getCostOfVertex(joinEdge.getRightVertex()) * factor;
    } else {
      // make cost bigger if cross join
      cost = Math.pow(getCostOfVertex(joinEdge.getLeftVertex()) *
          getCostOfVertex(joinEdge.getRightVertex()), 2);
    }

    return checkInfinity(cost * COMPUTATION_FACTOR);
  }

  public static double getCostOfVertex(JoinVertex joinVertex) {
    double cost;
    if (joinVertex instanceof RelationVertex) {
      cost = getCost(((RelationVertex) joinVertex).getRelationNode());
    } else {
      cost = getCost(((JoinedRelationsVertex)joinVertex).getJoinEdge());
    }
    return cost;
  }

  /**
   * Return the MAX(MIN) value if the given cost is positive(negative) infinity.
   *
   * @param cost
   * @return
   */
  private static double checkInfinity(double cost) {
    if (cost == Double.POSITIVE_INFINITY) {
      return Long.MAX_VALUE;
    } else if (cost == Double.NEGATIVE_INFINITY) {
      return Long.MIN_VALUE;
    } else {
      return cost;
    }
  }

  // TODO - costs of other operator operators (e.g., group-by and sort) should be computed in proper manners.
  public static double getCost(LogicalNode node) {
    double cost;
    switch (node.getType()) {

    case PROJECTION:
      ProjectionNode projectionNode = (ProjectionNode) node;
      cost = getCost(projectionNode.getChild());
      break;

    case JOIN:
      JoinNode joinNode = (JoinNode) node;
      double filterFactor = 1;
      if (joinNode.hasJoinQual()) {
        filterFactor = Math.pow(DEFAULT_SELECTION_FACTOR,
            AlgebraicUtil.toConjunctiveNormalFormArray(joinNode.getJoinQual()).length);
        cost = getCost(joinNode.getLeftChild()) *
            getCost(joinNode.getRightChild()) * filterFactor;
      } else {
        cost = Math.pow(getCost(joinNode.getLeftChild()) *
            getCost(joinNode.getRightChild()), 2);
      }
      break;

    case SELECTION:
      SelectionNode selectionNode = (SelectionNode) node;
      cost = getCost(selectionNode.getChild()) *
          Math.pow(DEFAULT_SELECTION_FACTOR, AlgebraicUtil.toConjunctiveNormalFormArray(selectionNode.getQual()).length);
      break;

    case TABLE_SUBQUERY:
      TableSubQueryNode subQueryNode = (TableSubQueryNode) node;
      cost = getCost(subQueryNode.getSubQuery());
      break;

    case SCAN:
      ScanNode scanNode = (ScanNode) node;
      if (scanNode.getTableDesc().getStats() != null) {
        cost = ((ScanNode)node).getTableDesc().getStats().getNumBytes();
      } else {
        cost = Integer.MAX_VALUE;
      }
      break;

    case UNION:
      UnionNode unionNode = (UnionNode) node;
      cost = getCost(unionNode.getLeftChild()) +
          getCost(unionNode.getRightChild());
      break;

    case EXCEPT:
    case INTERSECT:
      throw new UnsupportedOperationException("getCost() does not support EXCEPT or INTERSECT yet");

    default:
      // all binary operators (join, union, except, and intersect) are handled in the above cases.
      // So, we need to handle only unary nodes in default.
      cost = getCost(((UnaryNode) node).getChild());
      break;
    }

    return cost * COMPUTATION_FACTOR;
  }
}