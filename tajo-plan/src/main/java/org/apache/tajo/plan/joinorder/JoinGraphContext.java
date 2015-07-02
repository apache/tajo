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

import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinSpec;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class JoinGraphContext {
  private Set<JoinVertex> rootVertexes = TUtil.newHashSet(); // most left vertex in the join plan
  private JoinGraph joinGraph = new JoinGraph();
  // New join edges are frequently created during join order optimization.
  // This cache is to reduce the overhead of join edge creation.
  private Map<Pair<JoinVertex,JoinVertex>, JoinEdge> edgeCache = TUtil.newHashMap();
  private Pair<JoinVertex,JoinVertex> cacheKey = new Pair<JoinVertex, JoinVertex>(); // Join
  private Set<EvalNode> candidateJoinConditions = TUtil.newHashSet(); // predicates from the on clause
  private Set<EvalNode> candidateJoinFilters = TUtil.newHashSet();    // predicates from the where clause
  private Set<EvalNode> evaluatedJoinConditions = TUtil.newHashSet(); // predicates from the on clause
  private Set<EvalNode> evaluatedJoinFilters = TUtil.newHashSet();    // predicates from the where clause

  public JoinGraph getJoinGraph() {
    return joinGraph;
  }

  public void addCandidateJoinConditions(Collection<EvalNode> candidates) {
    for (EvalNode eachCandidate : candidates) {
      if (!evaluatedJoinConditions.contains(eachCandidate)) {
        candidateJoinConditions.add(eachCandidate);
      }
    }
  }

  public void addCandidateJoinFilters(Collection<EvalNode> candidates) {
    for (EvalNode eachCandidate : candidates) {
      if (!evaluatedJoinFilters.contains(eachCandidate)) {
        candidateJoinFilters.add(eachCandidate);
      }
    }
  }

  public void removeCandidateJoinConditions(Collection<EvalNode> willBeRemoved) {
    candidateJoinConditions.remove(willBeRemoved);
  }

  public void removeCandidateJoinFilters(Collection<EvalNode> willBeRemoved) {
    candidateJoinFilters.remove(willBeRemoved);
  }

  public void markAsEvaluatedJoinConditions(Collection<EvalNode> willBeMarked) {
    for (EvalNode eachEval : willBeMarked) {
      if (candidateJoinConditions.contains(eachEval)) {
        candidateJoinConditions.remove(eachEval);
        evaluatedJoinConditions.add(eachEval);
      }
    }
  }

  public void markAsEvaluatedJoinFilters(Collection<EvalNode> willBeMarked) {
    for (EvalNode eachEval : willBeMarked) {
      if (candidateJoinFilters.contains(eachEval)) {
        candidateJoinFilters.remove(eachEval);
        evaluatedJoinFilters.add(eachEval);
      }
    }
  }

  public Set<EvalNode> getCandidateJoinConditions() {
    return candidateJoinConditions;
  }

  public Set<EvalNode> getCandidateJoinFilters() {
    return candidateJoinFilters;
  }

  public Set<EvalNode> getEvaluatedJoinConditions() {
    return evaluatedJoinConditions;
  }

  public Set<EvalNode> getEvaluatedJoinFilters() {
    return evaluatedJoinFilters;
  }

  public Set<JoinVertex> getRootVertexes() {
    return rootVertexes;
  }

  public void addRootVertexes(JoinVertex rootVertex) {
    this.rootVertexes.add(rootVertex);
  }

  public boolean removeRootVertexes(JoinVertex rootVertex) {
    return this.rootVertexes.remove(rootVertex);
  }

  public void replaceRootVertexes(JoinVertex oldRoot, JoinVertex newRoot) {
    removeRootVertexes(oldRoot);
    addRootVertexes(newRoot);
  }

  public JoinEdge cacheEdge(JoinEdge edge) {
    edgeCache.put(new Pair<JoinVertex, JoinVertex>(edge.getLeftVertex(), edge.getRightVertex()), edge);
    return edge;
  }

  public JoinEdge getCachedOrNewJoinEdge(JoinSpec joinSpec, JoinVertex left, JoinVertex right) {
    cacheKey.set(left, right);
    if (edgeCache.containsKey(cacheKey)) {
      return edgeCache.get(cacheKey);
    } else {
      return cacheEdge(new JoinEdge(joinSpec, left, right));
    }
  }
}