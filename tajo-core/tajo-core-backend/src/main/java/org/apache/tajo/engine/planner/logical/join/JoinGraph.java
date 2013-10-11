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

package org.apache.tajo.engine.planner.logical.join;

import com.google.common.collect.Sets;
import org.apache.tajo.algebra.JoinType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.planner.PlannerUtil;
import org.apache.tajo.engine.planner.graph.SimpleUndirectedGraph;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class JoinGraph extends SimpleUndirectedGraph<String, JoinEdge> {
  public Collection<JoinEdge> getJoinsWith(String relation) {
    return getEdges(relation);
  }

  public Collection<EvalNode> addJoin(JoinType joinType, EvalNode joinQual) {
    Set<EvalNode> cnf = Sets.newHashSet(EvalTreeUtil.getConjNormalForm(joinQual));
    Set<EvalNode> nonJoinQuals = Sets.newHashSet();
    for (EvalNode singleQual : cnf) {
      if (PlannerUtil.isJoinQual(singleQual)) {
        List<Column> left = EvalTreeUtil.findAllColumnRefs(singleQual.getLeftExpr());
        List<Column> right = EvalTreeUtil.findAllColumnRefs(singleQual.getRightExpr());

        String leftRelName = left.get(0).getQualifier();
        String rightRelName = right.get(0).getQualifier();

        JoinEdge edge = getEdge(leftRelName, rightRelName);

        if (edge != null) {
          edge.addJoinQual(singleQual);
        } else {
          edge = new JoinEdge(joinType, leftRelName, rightRelName, singleQual);
          addEdge(leftRelName, rightRelName, edge);
        }
      } else {
        nonJoinQuals.add(singleQual);
      }
    }
    cnf.retainAll(nonJoinQuals);
    return cnf;
  }
}
