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

package org.apache.tajo.plan.joinorder;

import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.JoinSpec;
import org.apache.tajo.util.graph.SimpleUndirectedGraph;

/**
 * A join graph must be the connected graph
 */
public class JoinGraph extends SimpleUndirectedGraph<JoinVertex, JoinEdge> {
//
//  private final JoinGraphContext context;
//
//  public JoinGraph(JoinGraphContext context) {
//    this.context = context;
//    this.context.setJoinGraph(this);
//  }

  public JoinEdge addJoin(JoinGraphContext context, JoinSpec joinSpec, JoinVertex left, JoinVertex right) throws PlanningException {
    JoinEdge edge = context.getCachedOrNewJoinEdge(joinSpec, left, right);
    this.addEdge(left, right, edge);
    return edge;
  }

//  private JoinEdge updateJoinPredicates(JoinEdge edge) {
//    if (edge != null) {
//      Set<EvalNode> additionalPredicates = JoinOrderingUtil.findJoinConditionForJoinVertex(
//          context.getCandidateJoinConditions(), edge, true);
//      additionalPredicates.addAll(JoinOrderingUtil.findJoinConditionForJoinVertex(
//          context.getCandidateJoinFilters(), edge, false));
//      context.removeCandidateJoinConditions(additionalPredicates);
//      context.removeCandidateJoinFilters(additionalPredicates);
//      return JoinOrderingUtil.addPredicates(edge, additionalPredicates);
//    }
//    return null;
//  }
//
//  @Override
//  public JoinEdge getEdge(JoinVertex tail, JoinVertex head) {
//    return updateJoinPredicates(super.getEdge(tail, head));
//  }
//
//  @Override
//  public List<JoinEdge> getIncomingEdges(JoinVertex vertex) {
//    List<JoinEdge> edges = super.getIncomingEdges(vertex);
//    if (edges != null) {
//      for (JoinEdge edge : edges) {
//        updateJoinPredicates(edge);
//      }
//    }
//    return edges;
//  }
//
//  @Override
//  public List<JoinEdge> getOutgoingEdges(JoinVertex vertex) {
//    List<JoinEdge> edges = super.getOutgoingEdges(vertex);
//    if (edges != null) {
//      for (JoinEdge edge : edges) {
//        updateJoinPredicates(edge);
//      }
//    }
//    return edges;
//  }
//
//  @Override
//  public Collection<JoinEdge> getEdgesAll() {
//    Collection<JoinEdge> edges = super.getEdgesAll();
//    if (edges != null) {
//      for (JoinEdge edge : edges) {
//        updateJoinPredicates(edge);
//      }
//    }
//    return edges;
//  }
//
//  @Override
//  public JoinEdge getReverseEdge(JoinVertex head, JoinVertex tail) {
//    return updateJoinPredicates(super.getReverseEdge(head, tail));
//  }
//
//  public JoinGraphContext getContext() {
//    return context;
//  }
}
