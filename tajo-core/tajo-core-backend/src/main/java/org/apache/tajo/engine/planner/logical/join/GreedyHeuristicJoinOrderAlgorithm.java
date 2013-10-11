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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.logical.JoinNode;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.planner.logical.RelationNode;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.engine.utils.SchemaUtil;
import org.apache.tajo.util.TUtil;

import java.util.*;

/**
 * This is a greedy heuristic algorithm to find a left-deep join tree. This algorithm finds
 * the best join order with join conditions and pushes down join conditions to
 * all join operators.
 */
public class GreedyHeuristicJoinOrderAlgorithm implements JoinOrderAlgorithm {
  public static double DEFAULT_SELECTION_FACTOR = 0.1;

  @Override
  public FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraph joinGraph,
                                         Set<EvalNode> qualSet, Set<String> relationsWithoutQual) {

    // Build a map (a relation name -> a relation object)
    // initialize a relation set
    HashMap<String, RelationNode> relationMap = Maps.newHashMap();
    // Remain relation set to be joined
    Set<String> remainRelNames = new HashSet<String>();
    for (RelationNode relation : block.getRelations()) {
      RelationNode rel = relation;
      relationMap.put(rel.getCanonicalName(), rel);
      remainRelNames.add(rel.getCanonicalName());
    }
    // A set of already-joined relations
    Set<String> alreadyJoinedRelNames = new HashSet<String>();

    // Get candidates from all relations to be joined
    List<RelationNode> candidates = getSmallerRelations(relationMap, remainRelNames);

    LogicalNode lastOne = candidates.get(0); // Get the first candidate relation (i.e., smallest)
    remainRelNames.remove(((RelationNode)lastOne).getCanonicalName()); // Remove the first candidate relation

    // Add the first candidate to the set of joined relations
    alreadyJoinedRelNames.add(((RelationNode)lastOne).getCanonicalName());

    List<CandidateJoinNode> orderedJoins;
    CandidateJoinNode chosen = null;
    while(true) {
      // Get a set of relations that can be joined to the composite relation.
      Set<CandidateJoin> joinCandidates = new HashSet<CandidateJoin>();
      for (String currentRel : alreadyJoinedRelNames) {
        // find all relations that can be joined to this relation
        Collection<JoinEdge> edges = joinGraph.getJoinsWith(currentRel);

        if (edges.size() > 0) { // if there are available join quals
          for (JoinEdge edge : edges) {
            if (alreadyJoinedRelNames.contains(edge.getLeftRelation())
                && currentRel.contains(edge.getRightRelation())) { // if two relations are already joined
              continue;
            }

            if (!alreadyJoinedRelNames.contains(edge.getLeftRelation())) {
              joinCandidates.add(new CandidateJoin(edge.getLeftRelation(), edge)) ;
            }
            if (!alreadyJoinedRelNames.contains(edge.getRightRelation())) {
              joinCandidates.add(new CandidateJoin(edge.getRightRelation(), edge));
            }
          }
        } else {
          for (RelationNode rel : block.getRelations()) {
            // Add all relations except for itself to join candidates
            if (!currentRel.equals(rel.getCanonicalName())) {
              joinCandidates.add(new CandidateJoin(rel.getCanonicalName(),
                  new JoinEdge(JoinType.CROSS, currentRel, rel.getCanonicalName())));
            }
          }
        }
      }

      // Get a ranked candidates from the set of relations that can be joined
      orderedJoins = getBestJoinRelations(relationMap, lastOne, joinCandidates);

      // Get a candidate relation such that the candidate incurs the smallest intermediate and is
      // not join to any relation yet.

      for (int i = 0; i < orderedJoins.size(); i++) {
        chosen = orderedJoins.get(i);
        if (remainRelNames.contains(chosen.getRelation().getCanonicalName())) {
          break;
        }
      }

      // Set the candidate to a inner relation and remove from the relation set.
      JoinNode lastJoinNode = new JoinNode(plan.newPID(), chosen.getJoinType());
      lastJoinNode.setLeftChild(lastOne); // Set the first candidate to a left relation of the first join
      lastJoinNode.setRightChild(chosen.getRelation());

      Schema merged = SchemaUtil.merge(lastJoinNode.getLeftChild().getOutSchema(),
          lastJoinNode.getRightChild().getOutSchema());
      lastJoinNode.setInSchema(merged);
      lastJoinNode.setOutSchema(merged);

      if (chosen.hasJoinQual()) {
        lastJoinNode.setJoinQual(EvalTreeUtil.transformCNF2Singleton(chosen.getJoinQual()));
        for (EvalNode joinCondition : chosen.getJoinQual()) {
          qualSet.remove(joinCondition);
        }
      }
      lastJoinNode.setCost(getCost(chosen));
      alreadyJoinedRelNames.add(chosen.getRelation().getCanonicalName());
      remainRelNames.remove(chosen.getRelation().getCanonicalName());
      lastOne = lastJoinNode;

      // If the relation set is empty, stop this loop.
      if (remainRelNames.isEmpty()) {
        Preconditions.checkState(qualSet.isEmpty(), "Not all join conditions are pushed down to joins.");
        break;
      }
    }

    return new FoundJoinOrder((JoinNode) lastOne, qualSet.toArray(new EvalNode[qualSet.size()]), getCost(chosen));
  }

  private class CandidateJoin {
    final JoinEdge edge;
    final String relationName;

    public CandidateJoin(String rightRelation, JoinEdge edge) {
      this.relationName = rightRelation;
      this.edge = edge;
    }

    public String getRelationName() {
      return relationName;
    }

    public JoinEdge getEdge() {
      return edge;
    }

    public String toString() {
      return edge.toString();
    }
  }

  private class CandidateJoinNode {
    final LogicalNode relOrJoin;
    final RelationNode rel;
    final JoinEdge joinEdge;

    public CandidateJoinNode(LogicalNode relOrJoin, RelationNode relation, JoinEdge joinEdge) {
      this.relOrJoin = relOrJoin;
      this.rel = relation;
      this.joinEdge = joinEdge;
    }

    public LogicalNode getRelationOrJoin() {
      return relOrJoin;
    }

    public RelationNode getRelation() {
      return rel;
    }

    public JoinType getJoinType() {
      return joinEdge.getJoinType();
    }

    public boolean hasJoinQual() {
      return joinEdge.hasJoinQual() && joinEdge.getJoinQual().length > 0;
    }

    public EvalNode [] getJoinQual() {
      return joinEdge.getJoinQual();
    }

    public String toString() {
      StringBuilder sb = new StringBuilder(rel.getCanonicalName());
      if (hasJoinQual()) {
        sb.append("with ").append(TUtil.arrayToString(joinEdge.getJoinQual()));
      }
      return sb.toString();
    }
  }

  private List<RelationNode> getSmallerRelations(Map<String, RelationNode> map, Set<String> relationSet) {
    List<RelationNode> relations = new ArrayList<RelationNode>();
    for (String name : relationSet) {
      relations.add(map.get(name));
    }
    Collections.sort(relations, new RelationOpComparator());
    return relations;
  }

  private List<CandidateJoinNode> getBestJoinRelations(Map<String, RelationNode> map,
                                                       LogicalNode lastJoin,
                                                       Set<CandidateJoin> candidateJoins) {
    List<CandidateJoinNode> relations = new ArrayList<CandidateJoinNode>();
    for (CandidateJoin candidate : candidateJoins) {
      relations.add(new CandidateJoinNode(lastJoin, map.get(candidate.getRelationName()), candidate.getEdge()));
    }
    Collections.sort(relations, new CandidateJoinOpComparator());
    return relations;
  }

  public static double getCost(LogicalNode node) {
    if (node instanceof ScanNode) {
      ScanNode scanNode = (ScanNode) node;
      if (scanNode.getTableDesc().getMeta().getStat() != null) {
        return ((ScanNode)node).getTableDesc().getMeta().getStat().getNumBytes();
      } else {
        return Long.MAX_VALUE;
      }
    } else {
      return node.getCost();
    }
  }

  /** it assumes that left-deep join tree. */
  public static double getCost(LogicalNode left, LogicalNode right, EvalNode [] quals) {
    double filterFactor = 1;
    if (quals != null) {
      filterFactor = Math.pow(DEFAULT_SELECTION_FACTOR, quals.length);
    }

    if (left instanceof RelationNode) {
      return getCost(left) * getCost(right) * filterFactor;
    } else {
      return getCost(left)
          + (getCost(left) * getCost(right) * filterFactor);
    }
  }

  private double getCost(CandidateJoinNode join) {
    return getCost(join.getRelationOrJoin(), join.getRelation(), join.getJoinQual());
  }

  class RelationOpComparator implements Comparator<RelationNode> {
    @Override
    public int compare(RelationNode o1, RelationNode o2) {
      if (getCost(o1) < getCost(o2)) {
        return -1;
      } else if (getCost(o1) > getCost(o2)) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  class CandidateJoinOpComparator implements Comparator<CandidateJoinNode> {
    @Override
    public int compare(CandidateJoinNode o1, CandidateJoinNode o2) {
      double cmp = getCost(o1) - getCost(o2);
      if (cmp < 0) {
        return -1;
      } else if (getCost(o1.getRelation()) > getCost(o2.getRelation())) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}