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
import org.apache.tajo.catalog.Column;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.expr.EvalTreeUtil;
import org.apache.tajo.plan.expr.EvalType;
import org.apache.tajo.plan.util.PlannerUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinOrderingUtil {

  /**
   * Find join conditions which can be evaluated at the given join edge. The result depends on the isOnPredicate flag
   * which represents the given set of predicates is from the on clause of not.
   *
   * @param candidates set of predicates
   * @param edge join edge
   * @param isOnPredicates flag to represent the candidates from the on clause of not
   * @return predicates which can be evaluated at the given join edge
   */
  public static Set<EvalNode> findJoinConditionForJoinVertex(Set<EvalNode> candidates, JoinEdge edge,
                                                             boolean isOnPredicates) {
    Set<EvalNode> conditionsForThisJoin = candidates.stream().filter(predicate -> EvalTreeUtil.isJoinQual(predicate, false)
      && checkIfEvaluatedAtEdge(predicate, edge, isOnPredicates)).collect(Collectors.toSet());
    return conditionsForThisJoin;
  }

  /**
   * Check whether the given predicate can be evaluated at the given join edge or not. The result depends on
   * the isOnPredicate flag which represents the given predicate is from the on clause of not.
   *
   * @param evalNode predicate
   * @param edge join edge
   * @param isOnPredicate flag to represent the candidates from the on clause of not
   * @return true if the predicate can be evaluated at the given join edge
   */
  public static boolean checkIfEvaluatedAtEdge(EvalNode evalNode, JoinEdge edge, boolean isOnPredicate) {
    Set<Column> columnRefs = EvalTreeUtil.findUniqueColumns(evalNode);
    if (EvalTreeUtil.findDistinctAggFunction(evalNode).size() > 0) {
      return false;
    }
    if (EvalTreeUtil.findEvalsByType(evalNode, EvalType.WINDOW_FUNCTION).size() > 0) {
      return false;
    }
    if (columnRefs.size() > 0 && !edge.getSchema().containsAll(columnRefs)) {
      return false;
    }
    // Currently, join filters cannot be evaluated at joins
    if (PlannerUtil.isOuterJoinType(edge.getJoinType()) && !isOnPredicate) {
      return false;
    }
    return true;
  }

  /**
   * Check the associativity between the given two join edges.
   * For the associativity rules, please refer to the below isAssociativeJoinType() function.
   *
   * @param context join graph context
   * @param leftEdge left join edge
   * @param rightEdge right join edge
   * @return true if two given join edges are associative.
   */
  public static boolean isAssociativeJoin(JoinGraphContext context, JoinEdge leftEdge, JoinEdge rightEdge) {
    if (isAssociativeJoinType(leftEdge.getJoinType(), rightEdge.getJoinType())) {

      // NOTE: There will be more predicates which are able to be evaluated at input join edges.
      // In this case, the input edges are not considered as associative joins to evaluate those predicates at proper
      // join edges, even though they have the associative relationship.

      // Create a temporal left-deep join node to find the potentially evaluatable quals.
      JoinedRelationsVertex tempLeftChild = new JoinedRelationsVertex(leftEdge);
      JoinEdge tempEdge = context.getCachedOrNewJoinEdge(rightEdge.getJoinSpec(), tempLeftChild,
          rightEdge.getRightVertex());
      if ((rightEdge.getJoinType() != JoinType.INNER && rightEdge.getJoinType() != JoinType.CROSS
          && rightEdge.getJoinType() != JoinType.LEFT_SEMI && rightEdge.getJoinType() != JoinType.LEFT_ANTI)
          || (leftEdge.getJoinType() != JoinType.INNER && leftEdge.getJoinType() != JoinType.CROSS
          && leftEdge.getJoinType() != JoinType.LEFT_SEMI && leftEdge.getJoinType() != JoinType.LEFT_ANTI)) {
        if (!findJoinConditionForJoinVertex(context.getCandidateJoinConditions(), tempEdge, true).isEmpty()) {
          return false;
        }
        if (!findJoinConditionForJoinVertex(context.getCandidateJoinFilters(), tempEdge, true).isEmpty()) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Check two join types are associative according to the following rule.
   *
   * <h3>Associativity rules</h3>
   *
   * ==============================================================
   * Left-Hand Bracketed  | Right-Hand Bracketed  | Equivalence
   * ==============================================================
   * (A inner B) inner C  | A inner (B inner C)   | Equivalent
   * (A left B) inner C   | A left (B inner C)    | Not equivalent
   * (A right B) inner C  | A right (B inner C)   | Equivalent
   * (A full B) inner C   | A full (B inner C)    | Not equivalent
   * (A inner B) left C   | A inner (B left C)    | Equivalent
   * (A left B) left C    | A left (B left C)     | Equivalent
   * (A right B) left C   | A right (B left C)    | Equivalent
   * (A full B) left C    | A full (B left C)     | Equivalent
   * (A inner B) right C  | A inner (B right C)   | Not equivalent
   * (A left B) right C   | A left (B right C)    | Not equivalent
   * (A right B) right C  | A right (B right C)   | Equivalent
   * (A full B) right C   | A full (B right C)    | Not equivalent
   * (A inner B) full C   | A inner (B full C)    | Not equivalent
   * (A left B) full C    | A left (B full C)     | Not equivalent
   * (A right B) full C   | A right (B full C)    | Equivalent
   * (A full B) full C    | A full (B full C)     | Equivalent
   * ==============================================================
   *
   * Cross, Semi and Anti joins follow the rule of the Inner join.
   *
   * @param leftType
   * @param rightType
   * @return true if two join types are associative.
   */
  public static boolean isAssociativeJoinType(JoinType leftType, JoinType rightType) {
    if (leftType == rightType) {
      return true;
    }

    boolean isLeftInner = leftType == JoinType.INNER || leftType == JoinType.CROSS
        || leftType == JoinType.LEFT_SEMI || leftType == JoinType.LEFT_ANTI;
    boolean isRightInner = rightType == JoinType.INNER || rightType == JoinType.CROSS
        || rightType == JoinType.LEFT_SEMI || rightType == JoinType.LEFT_ANTI;

    if (isLeftInner && isRightInner) {
      return true;
    }

    if (leftType == JoinType.RIGHT_OUTER) {
      return true;
    }

    if (leftType == JoinType.LEFT_OUTER) {
      // When the left type is the left outer join, input join types are associative
      // if the right type is also the left outer join.
      // This case is already checked above.
      return false;
    }

    if (isLeftInner) {
      if (rightType == JoinType.LEFT_OUTER) {
        return true;
      } else {
        return false;
      }
    }

    if (leftType == JoinType.FULL_OUTER) {
      if (rightType == JoinType.LEFT_OUTER) {
        return true;
      } else {
        return false;
      }
    }

    return false;
  }

  /**
   * Find all interchangeable vertexes from the given vertex.
   * Join edges between relations are found at runtime.
   *
   * <H3>Vertex interchange rules</H3>
   * - A vertex is interchangeable with the start vertex if it is reachable.
   * - A vertex is reachable from the start vertex if it is able to replace another vertex which is connected to the start vertex.
   * - A vertex is able to replace another vertex if they are connected without violating join commutativity or join associativity.
   *
   * @param context join graph context
   * @param from start vertex
   * @return
   */
  public static Set<JoinVertex> getAllInterchangeableVertexes(JoinGraphContext context, JoinVertex from) {
    Set<JoinVertex> founds = new HashSet<>();
    getAllInterchangeableVertexes(founds, context, from);
    return founds;
  }

  public static void getAllInterchangeableVertexes(Set<JoinVertex> founds, JoinGraphContext context, JoinVertex vertex) {
    founds.add(vertex);
    Set<JoinVertex> foundAtThis = new HashSet<>();
    List<JoinEdge> candidateEdges = context.getJoinGraph().getOutgoingEdges(vertex);
    if (candidateEdges != null) {
      for (JoinEdge candidateEdge : candidateEdges) {
        // Evaluatable quals must be added to check the associativity of join edges.
        candidateEdge = updateQualIfNecessary(context, candidateEdge);
        if (!founds.contains(candidateEdge.getRightVertex())) {
          List<JoinEdge> rightEdgesOfCandidate = context.getJoinGraph().getOutgoingEdges(candidateEdge.getRightVertex());
          boolean reacheable = false;
          if (rightEdgesOfCandidate != null) {
            reacheable = true;
            for (JoinEdge rightEdgeOfCandidate : rightEdgesOfCandidate) {
              // Evaluatable quals must be added to check the associativity of join edges.
              rightEdgeOfCandidate = updateQualIfNecessary(context, rightEdgeOfCandidate);
              if (!isAssociativeJoin(context, candidateEdge, rightEdgeOfCandidate)) {
                reacheable = false;
                break;
              }
            }
          }
          if (reacheable) {
            foundAtThis.add(candidateEdge.getRightVertex());
          }
        }
      }
      for (JoinVertex v : foundAtThis) {
        getAllInterchangeableVertexes(founds, context, v);
      }
    }
  }

  /**
   * Check the given two join edges are equal or symmetric.
   *
   * @param edge1
   * @param edge2
   * @return True if two join edges are equal or symmetric.
   */
  public static boolean isEqualsOrSymmetric(JoinEdge edge1, JoinEdge edge2) {
    if (edge1.equals(edge2) || isSymmetric(edge1, edge2)) {
      return true;
    }
    return false;
  }

  /**
   * Given two join edges e1 and e2, they are <b>symmetric</b> whey they satisfy the follwing conditions.
   *
   * <ul>
   *   <li>e1 and e2 have the same commutative join type.</li>
   *   <li>e1 and e2 have the same join condition.</li>
   *   <li>The left and right vertexes of e1 are the right and left vertexes of e2, respectively.</li>
   * </ul>
   *
   * @param edge1
   * @param edge2
   * @return True if two join edges are symmetric.
   */
  public static boolean isSymmetric(JoinEdge edge1, JoinEdge edge2) {
    if (edge1.getLeftVertex().equals(edge2.getRightVertex()) &&
        edge1.getRightVertex().equals(edge2.getLeftVertex()) &&
        edge1.getJoinSpec().equals(edge2.getJoinSpec()) &&
        PlannerUtil.isCommutativeJoinType(edge1.getJoinType())) {
      return true;
    }
    return false;
  }

  /**
   * If there are predicates which can be evaluated at the given join edge, push those predicates to the join edge.
   *
   * @param context
   * @param edge
   * @return
   */
  public static JoinEdge updateQualIfNecessary(JoinGraphContext context, JoinEdge edge) {
    Set<EvalNode> additionalPredicates = findJoinConditionForJoinVertex(
        context.getCandidateJoinConditions(), edge, true);
    additionalPredicates.addAll(findJoinConditionForJoinVertex(
        context.getCandidateJoinFilters(), edge, false));
    edge.addJoinPredicates(additionalPredicates);
    return edge;
  }

  /**
   * Find all edges that are associative with the given edge.
   *
   * @param context
   * @param edge
   * @return
   */
  public static Set<JoinEdge> getAllAssociativeEdges(JoinGraphContext context, JoinEdge edge) {
    Set<JoinEdge> associativeEdges = new HashSet<>();
    JoinVertex start = edge.getRightVertex();
    List<JoinEdge> candidateEdges = context.getJoinGraph().getOutgoingEdges(start);
    if (candidateEdges != null) {
      for (JoinEdge candidateEdge : candidateEdges) {
        candidateEdge = updateQualIfNecessary(context, candidateEdge);
        if (!isEqualsOrSymmetric(edge, candidateEdge) &&
            isAssociativeJoin(context, edge, candidateEdge)) {
          associativeEdges.add(candidateEdge);
        }
      }
    }
    return associativeEdges;
  }
}
