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

import org.apache.commons.collections.map.LRUMap;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.JoinSpec;
import org.apache.tajo.util.Pair;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class JoinGraphContext {
  private Set<JoinVertex> rootVertexes = new HashSet<>(); // most left vertex in the join plan
  private JoinGraph joinGraph = new JoinGraph();

  // New join edges are frequently created during join order optimization.
  // This cache is to reduce such overhead.
  private LRUMap edgeCache = new LRUMap(10000);

  // candidate predicates contain the predicates which are not pushed to any join nodes yet.
  // evaluated predicates contain the predicates which are already pushed to some join nodes.
  private Set<EvalNode> candidateJoinConditions = new HashSet<>(); // predicates from the on clause
  private Set<EvalNode> candidateJoinFilters = new HashSet<>();    // predicates from the where clause
  private Set<EvalNode> evaluatedJoinConditions = new HashSet<>(); // predicates from the on clause
  private Set<EvalNode> evaluatedJoinFilters = new HashSet<>();    // predicates from the where clause

  public JoinGraph getJoinGraph() {
    return joinGraph;
  }

  public void addCandidateJoinConditions(Collection<EvalNode> candidates) {
    candidateJoinConditions.addAll(candidates.stream().filter(eachCandidate -> !evaluatedJoinConditions.contains(eachCandidate)).collect(Collectors.toList()));
  }

  public void addCandidateJoinFilters(Collection<EvalNode> candidates) {
    candidateJoinFilters.addAll(candidates.stream().filter(eachCandidate -> !evaluatedJoinFilters.contains(eachCandidate)).collect(Collectors.toList()));
  }

  public void removeCandidateJoinConditions(Collection<EvalNode> willBeRemoved) {
    candidateJoinConditions.remove(willBeRemoved);
  }

  public void removeCandidateJoinFilters(Collection<EvalNode> willBeRemoved) {
    candidateJoinFilters.remove(willBeRemoved);
  }

  public void markAsEvaluatedJoinConditions(Collection<EvalNode> willBeMarked) {
    willBeMarked.stream().filter(eachEval -> candidateJoinConditions.contains(eachEval)).forEach(eachEval -> {
      candidateJoinConditions.remove(eachEval);
      evaluatedJoinConditions.add(eachEval);
    });
  }

  public void markAsEvaluatedJoinFilters(Collection<EvalNode> willBeMarked) {
    willBeMarked.stream().filter(eachEval -> candidateJoinFilters.contains(eachEval)).forEach(eachEval -> {
      candidateJoinFilters.remove(eachEval);
      evaluatedJoinFilters.add(eachEval);
    });
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
    edgeCache.put(new Pair<>(edge.getLeftVertex(), edge.getRightVertex()), edge);
    return edge;
  }

  public JoinEdge getCachedOrNewJoinEdge(JoinSpec joinSpec, JoinVertex left, JoinVertex right) {
    Pair<JoinVertex,JoinVertex> cacheKey = new Pair<>(left, right);
    if (edgeCache.containsKey(cacheKey)) {
      return (JoinEdge) edgeCache.get(cacheKey);
    } else {
      return cacheEdge(new JoinEdge(joinSpec, left, right));
    }
  }

  public void clear() {
    rootVertexes.clear();
    candidateJoinConditions.clear();
    candidateJoinFilters.clear();
    evaluatedJoinConditions.clear();
    evaluatedJoinFilters.clear();
    edgeCache.clear();
    joinGraph.clear();

    rootVertexes = null;
    candidateJoinConditions = null;
    candidateJoinFilters = null;
    evaluatedJoinConditions = null;
    evaluatedJoinFilters = null;
    edgeCache = null;
    joinGraph = null;
  }
}